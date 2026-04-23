package spdk

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

func discoverAndConnectNVMeTarget(srcIP string, srcPort int32, maxRetries int, retryInterval time.Duration) (subsystemNQN, controllerName string, err error) {
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to create executor")
	}

	err = retry.Do(
		func() error {
			var e error
			subsystemNQN, e = initiator.DiscoverTarget(srcIP, strconv.Itoa(int(srcPort)), executor)
			if e != nil {
				return errors.Wrapf(e, "discover target %s:%d failed", srcIP, srcPort)
			}

			controllerName, e = initiator.ConnectTarget(srcIP, strconv.Itoa(int(srcPort)), subsystemNQN, executor)
			if e != nil {
				return errors.Wrapf(e, "connect target %s:%d (nqn=%s) failed", srcIP, srcPort, subsystemNQN)
			}

			return nil
		},
		retry.Attempts(uint(maxRetries)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			logrus.WithError(err).Warnf(
				"Retrying NVMe target connect: addr=%s:%d attempt=%d/%d next_wait=%s",
				srcIP, srcPort, n+1, maxRetries, retryInterval,
			)
		}),
	)

	if err != nil || subsystemNQN == "" || controllerName == "" {
		return "", "", errors.Wrapf(err, "timeout connecting target with address %v:%v", srcIP, srcPort)
	}

	return subsystemNQN, controllerName, nil
}

func exposeSnapshotLvolBdev(spdkClient *spdkclient.Client, lvsName, lvolName, ip string, port int32, transport NvmfTransportType, executor *commonns.Executor) (subsystemNQN, controllerName string, err error) {
	if transport == "" {
		transport = DefaultNvmfTransport
	}
	spdkTransport := transport.ToSPDKTransportType()

	bdevLvolList, err := spdkClient.BdevLvolGet(spdktypes.GetLvolAlias(lvsName, lvolName), 0)
	if err != nil {
		return "", "", err
	}
	if len(bdevLvolList) == 0 {
		return "", "", errors.Errorf("cannot find lvol bdev %v for backup", lvolName)
	}

	portStr := strconv.Itoa(int(port))
	err = spdkClient.StartExposeBdevWithTransport(helpertypes.GetNQN(lvolName), bdevLvolList[0].UUID, generateNGUID(lvolName), ip, portStr, spdkTransport)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to expose snapshot lvol bdev %v", lvolName)
	}

	transportStr := string(transport)
	for r := 0; r < maxRetries; r++ {
		subsystemNQN, err = initiator.DiscoverTargetWithTransport(transportStr, ip, portStr, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to discover target for snapshot lvol bdev %v", lvolName)
			time.Sleep(retryInterval)
			continue
		}

		controllerName, err = initiator.ConnectTargetWithTransport(transportStr, ip, portStr, subsystemNQN, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to connect target for snapshot lvol bdev %v", lvolName)
			time.Sleep(retryInterval)
			continue
		}
		// break when it successfully discover and connect the target
		break
	}
	return subsystemNQN, controllerName, nil
}

func splitHostPort(address string) (string, int32, error) {
	if strings.Contains(address, ":") {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return "", 0, errors.Wrapf(err, "failed to split host and port from address %v", address)
		}

		portAsInt := 0
		if port != "" {
			portAsInt, err = strconv.Atoi(port)
			if err != nil {
				return "", 0, errors.Wrapf(err, "failed to parse port %v", port)
			}
		}
		return host, int32(portAsInt), nil
	}

	return address, 0, nil
}

func connectNVMfBdev(spdkClient *spdkclient.Client, controllerName, address string, ctrlrLossTimeout, fastIOFailTimeoutSec int, maxRetries int, retryInterval time.Duration) (bdevName string, err error) {
	return connectNVMfBdevWithTransport(spdkClient, controllerName, address, DefaultNvmfTransport, ctrlrLossTimeout, fastIOFailTimeoutSec, maxRetries, retryInterval)
}

func connectNVMfBdevWithTransport(spdkClient *spdkclient.Client, controllerName, address string, transport NvmfTransportType, ctrlrLossTimeout, fastIOFailTimeoutSec int, maxRetries int, retryInterval time.Duration) (bdevName string, err error) {
	if controllerName == "" || address == "" {
		return "", fmt.Errorf("controllerName or address is empty")
	}

	defer func() {
		if err != nil {
			if _, detachErr := spdkClient.BdevNvmeDetachController(controllerName); detachErr != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(detachErr) {
				logrus.WithError(detachErr).Errorf("Failed to detach NVMe controller %s after failing at attaching it", controllerName)
			}
		}
	}()

	ip, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}

	// Blindly detach the controller in case of the previous replica connection is not cleaned up correctly
	if _, err := spdkClient.BdevNvmeDetachController(controllerName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return "", err
	}

	nvmeBdevNameList := []string{}
	spdkTransport := transport.ToSPDKTransportType()
	err = retry.Do(
		func() error {
			var err error
			nvmeBdevNameList, err = spdkClient.BdevNvmeAttachController(
				controllerName,
				helpertypes.GetNQN(controllerName),
				ip,
				port,
				spdkTransport,
				spdktypes.NvmeAddressFamilyIPv4,
				int32(ctrlrLossTimeout),
				int32(replicaReconnectDelaySec),
				int32(fastIOFailTimeoutSec),
				replicaMultipath,
			)
			return err
		},
		retry.Attempts(uint(maxRetries)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			logrus.WithError(err).Warnf(
				"Retrying NVMe bdev attach: controller=%s address=%s transport=%s attempt=%d/%d next_wait=%s",
				controllerName, address, transport, n+1, maxRetries, retryInterval,
			)
		}),
	)

	if err != nil {
		nvmeBdevNameList, err = attemptTCPFallback(spdkClient, controllerName, ip, port, ctrlrLossTimeout, fastIOFailTimeoutSec, transport, err)
		if err != nil {
			return "", fmt.Errorf("attach NVMe controller failed after %d attempts: %w", maxRetries, err)
		}
	}

	if len(nvmeBdevNameList) != 1 {
		return "", fmt.Errorf("got zero or multiple results when attaching lvol %s with address %s as a NVMe bdev: %+v", controllerName, address, nvmeBdevNameList)
	}

	return nvmeBdevNameList[0], nil
}

func attemptTCPFallback(spdkClient *spdkclient.Client, controllerName, ip, port string, ctrlrLossTimeout, fastIOFailTimeoutSec int, originalTransport NvmfTransportType, primaryErr error) ([]string, error) {
	primaryPort, parseErr := strconv.Atoi(port)
	if parseErr != nil {
		return nil, primaryErr
	}
	fallbackPort := strconv.Itoa(primaryPort + 1)

	if _, detachErr := spdkClient.BdevNvmeDetachController(controllerName); detachErr != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(detachErr) {
		return nil, primaryErr
	}

	logrus.WithError(primaryErr).Warnf(
		"Primary NVMe attach failed (controller=%s transport=%s address=%s:%s); trying TCP fallback on %s:%s",
		controllerName, originalTransport, ip, port, ip, fallbackPort,
	)

	list, err := spdkClient.BdevNvmeAttachController(
		controllerName,
		helpertypes.GetNQN(controllerName),
		ip,
		fallbackPort,
		spdktypes.NvmeTransportTypeTCP,
		spdktypes.NvmeAddressFamilyIPv4,
		int32(ctrlrLossTimeout),
		int32(replicaReconnectDelaySec),
		int32(fastIOFailTimeoutSec),
		replicaMultipath,
	)
	if err != nil {
		return nil, fmt.Errorf("primary attach failed (%v) and TCP fallback to %s:%s also failed: %w", primaryErr, ip, fallbackPort, err)
	}
	return list, nil
}

func disconnectNVMfBdev(spdkClient *spdkclient.Client, bdevName string, maxRetries int, retryInterval time.Duration) error {
	if bdevName == "" {
		return nil
	}

	controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(bdevName)

	return retry.Do(
		func() error {
			_, err := spdkClient.BdevNvmeDetachController(controllerName)
			if err != nil {
				if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
					return nil
				}
				if isRPCConnectionTimedOut(err) {
					logrus.WithError(err).Warnf("NVMe bdev detach timed out (controller=%s, remote target unresponsive); trusting ctrlr_loss_timeout to reap, skipping retry", controllerName)
					return nil
				}
				return err
			}
			return nil
		},
		retry.Attempts(uint(maxRetries)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			logrus.WithError(err).Warnf(
				"Retrying NVMe bdev detach: controller=%s attempt=%d/%d next_wait=%s",
				controllerName, n+1, maxRetries, retryInterval,
			)
		}),
	)
}

func isRPCConnectionTimedOut(err error) bool {
	if err == nil {
		return false
	}
	jsonRPCError, ok := err.(jsonrpc.JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*jsonrpc.ResponseError)
	if !ok {
		return false
	}
	return responseError.Code == -110
}

func GetSnapXattr(spdkClient *spdkclient.Client, alias, key string) (string, error) {
	value, err := spdkClient.BdevLvolGetXattr(alias, key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func GetLvsNameByUUID(spdkClient *spdkclient.Client, lvsUUID string) (string, error) {
	if lvsUUID == "" {
		return "", fmt.Errorf("empty UUID provided when getting logical volume store name")
	}
	var lvsList []spdktypes.LvstoreInfo
	lvsList, err := spdkClient.BdevLvolGetLvstore("", lvsUUID)
	if err != nil {
		return "", err
	}
	if len(lvsList) != 1 {
		return "", fmt.Errorf("expected exactly one lvstore for UUID %s, but found %d", lvsUUID, len(lvsList))
	}
	return lvsList[0].Name, nil
}

// ExtractBackingImageAndDiskUUID extracts the BackingImageName and DiskUUID from the string pattern "bi-${BackingImageName}-disk-${DiskUUID}"
func ExtractBackingImageAndDiskUUID(lvolName string) (string, string, error) {
	// Define the regular expression pattern
	// This captures the BackingImageName and DiskUUID while allowing for hyphens in both.
	re := regexp.MustCompile(`^bi-([a-zA-Z0-9-]+)-disk-([a-zA-Z0-9-]+)$`)

	// Try to find a match
	matches := re.FindStringSubmatch(lvolName)
	if matches == nil {
		return "", "", fmt.Errorf("lvolName does not match the expected pattern")
	}

	// Extract BackingImageName and DiskUUID from the matches
	backingImageName := matches[1]
	diskUUID := matches[2]

	return backingImageName, diskUUID, nil
}
