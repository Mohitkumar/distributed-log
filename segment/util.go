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

func Encode(entry *LogEntry) ([]byte, error) {
	buf := make([]byte, 12+len(entry.Value))
	endian.PutUint64(buf[0:8], entry.Offset)
	endian.PutUint32(buf[8:12], uint32(len(entry.Value)))
	copy(buf[12:], entry.Value)
	return buf, nil
}
