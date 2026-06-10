package types

type BdevRaidLevel string

const (
	BdevRaidLevel0      = BdevRaidLevel("0")
	BdevRaidLevelRaid0  = BdevRaidLevel("raid0")
	BdevRaidLevel1      = BdevRaidLevel("1")
	BdevRaidLevelRaid1  = BdevRaidLevel("raid1")
	BdevRaidLevel5f     = BdevRaidLevel("5f")
	BdevRaidLevelRaid5f = BdevRaidLevel("raid5f")
	BdevRaidLevelConcat = BdevRaidLevel("concat")
)

type BdevRaidInfo struct {
	Name                    string        `json:"name,omitempty"`
	StripSizeKb             uint32        `json:"strip_size_kb"`
	State                   string        `json:"state"`
	RaidLevel               BdevRaidLevel `json:"raid_level"`
	NumBaseBdevs            uint8         `json:"num_base_bdevs"`
	NumBaseBdevsDiscovered  uint8         `json:"num_base_bdevs_discovered"`
	NumBaseBdevsOperational uint8         `json:"num_base_bdevs_operational,omitempty"`
	BaseBdevsList           []BaseBdev    `json:"base_bdevs_list"`
	Superblock              bool          `json:"superblock"`
}

type BaseBdev struct {
	Name         string `json:"name"`
	UUID         string `json:"uuid"`
	IsConfigured bool   `json:"is_configured"`
	DataOffset   uint64 `json:"data_offset"`
	DataSize     uint64 `json:"data_size"`
}

type BdevRaidCreateRequest struct {
	Name        string        `json:"name"`
	RaidLevel   BdevRaidLevel `json:"raid_level"`
	StripSizeKb uint32        `json:"strip_size_kb"`
	BaseBdevs   []string      `json:"base_bdevs"`
	UUID        string        `json:"uuid,omitempty"`
	// DeltaBitmap enables per-base-bdev dirty-region tracking on raid1 so a
	// disconnected base bdev can be rebuilt incrementally (only dirty
	// regions) instead of a full resync after reconnect. Ignored for other
	// RAID levels. Requires each base bdev to have a non-zero
	// optimal_io_boundary — the region size is derived from
	// min(optimal_io_boundary) × blocklen.
	DeltaBitmap bool `json:"delta_bitmap,omitempty"`
}

type BdevRaidDeleteRequest struct {
	Name string `json:"name"`
}

type BdevRaidCategory string

const (
	BdevRaidCategoryAll         = "all"
	BdevRaidCategoryOnline      = "online"
	BdevRaidCategoryOffline     = "offline"
	BdevRaidCategoryConfiguring = "configuring"
)

type BdevRaidGetBdevsRequest struct {
	Category BdevRaidCategory `json:"category"`
}

type BdevRaidRemoveBaseBdevRequest struct {
	Name string `json:"name"`
}

type BdevRaidGrowBaseBdevRequest struct {
	RaidName string `json:"raid_name"`
	BaseName string `json:"base_name"`
}

// BdevRaidBaseBdevDeltaBitmapRequest is the common request for the three
// delta-bitmap RPCs (stop, get, clear). All three key off a base bdev name
// rather than a raid name because a faulty base bdev owns its own bitmap and
// faulty state.
type BdevRaidBaseBdevDeltaBitmapRequest struct {
	BaseBdevName string `json:"base_bdev_name"`
}

// BdevRaidBaseBdevDeltaBitmapResponse holds the bitmap payload returned by
// bdev_raid_get_base_bdev_delta_bitmap. DeltaBitmap is a base64-encoded
// spdk_bit_array; RegionSize is in bytes and equals
// raid.optimal_io_boundary × raid.blocklen.
type BdevRaidBaseBdevDeltaBitmapResponse struct {
	DeltaBitmap string `json:"delta_bitmap"`
	RegionSize  uint64 `json:"region_size"`
}
