package protocol

import "encoding/json"

// MarshalJSON marshals v to JSON (wrapper for encoding/json.Marshal).
func MarshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// UnmarshalJSON unmarshals data into v (wrapper for encoding/json.Unmarshal).
func UnmarshalJSON(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
