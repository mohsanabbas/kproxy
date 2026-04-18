package kwire

// API key constants for the requests we either intercept, rewrite, or use for
// telemetry collection. Other API keys flow through unchanged and are not
// listed here.
const (
	APIProduce          int16 = 0
	APIFetch            int16 = 1
	APIListOffsets      int16 = 2
	APIMetadata         int16 = 3
	APIOffsetCommit     int16 = 8
	APIOffsetFetch      int16 = 9
	APIFindCoordinator  int16 = 10
	APIJoinGroup        int16 = 11
	APIHeartbeat        int16 = 12
	APILeaveGroup       int16 = 13
	APISyncGroup        int16 = 14
	APIDescribeGroups   int16 = 15
	APIListGroups       int16 = 16
	APISaslHandshake    int16 = 17
	APIApiVersions      int16 = 18
	APISaslAuthenticate int16 = 36
)

// flexAt records the minimum API version at which each request and response
// uses the flexible (KIP-482) header. A value of -1 means the API never uses
// flexible headers.
//
// This table only needs entries for keys we actually parse on the proxy; for
// every other API key we route opaquely so we don't care which header version
// it uses.
//
// Refresh policy: when adding a new API key here, look it up in the Apache
// Kafka source under clients/src/main/resources/common/message/<Api>Request.json
// and pick the minimum version with "flexibleVersions" != "none". Same for the
// response file.
var flexAtRequest = map[int16]int16{
	APIProduce:          9,
	APIFetch:            12,
	APIListOffsets:      6,
	APIMetadata:         9,
	APIOffsetCommit:     8,
	APIOffsetFetch:      6,
	APIFindCoordinator:  3,
	APIJoinGroup:        6,
	APIHeartbeat:        4,
	APILeaveGroup:       4,
	APISyncGroup:        4,
	APIDescribeGroups:   5,
	APIListGroups:       3,
	APISaslHandshake:    -1, // SaslHandshake never uses flexible headers.
	APIApiVersions:      3,  // request flexible at v3
	APISaslAuthenticate: 2,
}

var flexAtResponse = map[int16]int16{
	APIProduce:          9,
	APIFetch:            12,
	APIListOffsets:      6,
	APIMetadata:         9,
	APIOffsetCommit:     8,
	APIOffsetFetch:      6,
	APIFindCoordinator:  3,
	APIJoinGroup:        6,
	APIHeartbeat:        4,
	APILeaveGroup:       4,
	APISyncGroup:        4,
	APIDescribeGroups:   5,
	APIListGroups:       3,
	APISaslHandshake:    -1,
	APIApiVersions:      -1, // ApiVersions response is never flexible.
	APISaslAuthenticate: 2,
}

// IsFlexibleRequest reports whether (apiKey,version) uses a flexible request
// header (header v2). Unknown keys default to non-flexible (header v1) so that
// passing through an unknown frame never causes us to mis-skip bytes — we just
// never decode the header in the first place.
func IsFlexibleRequest(apiKey, version int16) bool {
	if min, ok := flexAtRequest[apiKey]; ok && min >= 0 {
		return version >= min
	}
	return false
}

// IsFlexibleResponse reports whether (apiKey,version) uses a flexible response
// header.
func IsFlexibleResponse(apiKey, version int16) bool {
	if min, ok := flexAtResponse[apiKey]; ok && min >= 0 {
		return version >= min
	}
	return false
}
