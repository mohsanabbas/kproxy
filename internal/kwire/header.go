package kwire

// RequestHeader is the decoded prefix of a Kafka request frame. ClientID is
// borrowed from the input buffer when present (it's a Go string allocation,
// since we need to forward it across frame lifetimes if the body is rewritten).
type RequestHeader struct {
	APIKey     int16
	APIVersion int16
	CorrelID   int32
	ClientID   string // empty if header was v0 (no clientID at all) or null
	ClientNull bool   // true if the nullable clientID was explicitly null

	// HeaderSize is the number of bytes consumed from the start of the body to
	// reach the end of the header. The request payload begins at body[HeaderSize:].
	HeaderSize int
}

// DecodeRequestHeader parses an inbound request frame's header. body must start
// at the apiKey field (i.e. the length prefix has already been stripped by the
// frame reader).
func DecodeRequestHeader(body []byte) (RequestHeader, error) {
	c := NewCursor(body)
	h := RequestHeader{}
	apiKey, err := c.ReadInt16()
	if err != nil {
		return h, err
	}
	h.APIKey = apiKey
	apiVersion, err := c.ReadInt16()
	if err != nil {
		return h, err
	}
	h.APIVersion = apiVersion
	correlID, err := c.ReadInt32()
	if err != nil {
		return h, err
	}
	h.CorrelID = correlID

	// Per Kafka request header v2 spec (and confirmed by franz-go/librdkafka),
	// the clientID in the request header is ALWAYS encoded as a regular
	// NULLABLE_STRING (int16 length + bytes), not a COMPACT_NULLABLE_STRING,
	// even when the request itself is flexible. This is because clients send
	// ApiVersions before knowing the broker's supported versions and need to
	// remain compatible with old brokers that don't understand compact
	// strings. The "flexible" part of the header is only the trailing tagged
	// fields section (and only on the request side; ApiVersions response is
	// also non-flexible).
	s, isNull, err := c.ReadNullableString()
	if err != nil {
		return h, err
	}
	h.ClientID = s
	h.ClientNull = isNull
	if IsFlexibleRequest(apiKey, apiVersion) {
		if err := c.SkipTaggedFields(); err != nil {
			return h, err
		}
	}
	h.HeaderSize = c.Offset()
	return h, nil
}

// AppendRequestHeader writes a request header onto dst. It always emits the
// flexible (v2) form when the (apiKey,apiVersion) pair calls for it, otherwise
// the v1 form. We never emit v0.
func AppendRequestHeader(dst []byte, h RequestHeader) []byte {
	dst = AppendInt16(dst, h.APIKey)
	dst = AppendInt16(dst, h.APIVersion)
	dst = AppendInt32(dst, h.CorrelID)
	// See DecodeRequestHeader: clientID is always NULLABLE_STRING (non-compact),
	// even on flexible (v2) headers. Only the tagged-fields trailer is added
	// in the flexible case.
	dst = AppendNullableString(dst, h.ClientID, h.ClientNull)
	if IsFlexibleRequest(h.APIKey, h.APIVersion) {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

// ResponseHeader is the decoded prefix of a Kafka response frame.
type ResponseHeader struct {
	CorrelID   int32
	HeaderSize int
}

// DecodeResponseHeader parses a response frame header. The caller MUST already
// know the (apiKey,version) so that we can pick the right header version —
// responses don't carry that information themselves; routing comes from the
// correlation id.
func DecodeResponseHeader(body []byte, apiKey, apiVersion int16) (ResponseHeader, error) {
	c := NewCursor(body)
	correlID, err := c.ReadInt32()
	if err != nil {
		return ResponseHeader{}, err
	}
	if IsFlexibleResponse(apiKey, apiVersion) {
		if err := c.SkipTaggedFields(); err != nil {
			return ResponseHeader{}, err
		}
	}
	return ResponseHeader{CorrelID: correlID, HeaderSize: c.Offset()}, nil
}

// AppendResponseHeader writes a response header onto dst. The encoded form
// matches whatever shape the original (apiKey,apiVersion) requires.
func AppendResponseHeader(dst []byte, apiKey, apiVersion int16, correlID int32) []byte {
	dst = AppendInt32(dst, correlID)
	if IsFlexibleResponse(apiKey, apiVersion) {
		dst = AppendEmptyTaggedFields(dst)
	}
	return dst
}

// PeekResponseCorrelID extracts only the correlation id from a response frame.
// This works regardless of header flexibility because the correlation id is
// always the first 4 bytes.
func PeekResponseCorrelID(body []byte) (int32, error) {
	c := NewCursor(body)
	return c.ReadInt32()
}

// PeekRequestKeyVersion returns just the (apiKey, apiVersion) so the proxy can
// decide whether to intercept without paying the cost of decoding the full
// header or the body.
func PeekRequestKeyVersion(body []byte) (apiKey, apiVersion int16, err error) {
	c := NewCursor(body)
	apiKey, err = c.ReadInt16()
	if err != nil {
		return 0, 0, err
	}
	apiVersion, err = c.ReadInt16()
	if err != nil {
		return 0, 0, err
	}
	return apiKey, apiVersion, nil
}
