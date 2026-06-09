package types

type BdevDriverSpecificAio struct {
	FileName          string `json:"filename"`
	ReadOnly          bool   `json:"readonly"`
	BlockSizeOverride bool   `json:"block_size_override"`
}

type BdevAioDriverSpecificInfo struct {
	Filename          string `json:"filename"`
	BlockSizeOverride bool   `json:"block_size_override"`
	Readonly          bool   `json:"readonly"`
}

type BdevAioCreateRequest struct {
	Name      string `json:"name"`
	Filename  string `json:"filename"`
	BlockSize uint64 `json:"block_size,omitzero"`
	// NoWait is tri-state: nil omits the field so SPDK applies its built-in
	// default, while a non-nil pointer sends the value explicitly. A plain
	// bool with omitempty could never send false, which matters because the
	// pinned SPDK (v25.09) defaults nowait to on.
	NoWait *bool `json:"nowait,omitempty"`
}

type BdevAioDeleteRequest struct {
	Name string `json:"name"`
}
