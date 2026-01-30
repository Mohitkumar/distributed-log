package segment

type LogEntry struct {
	Offset uint64
	Value  []byte
}

func Decode(data []byte) (*LogEntry, error) {
	if len(data) < 12 {
		return &LogEntry{Value: data}, nil
	}
	offset := endian.Uint64(data[0:8])
	return &LogEntry{
		Offset: offset,
		Value:  data[8:],
	}, nil
}
