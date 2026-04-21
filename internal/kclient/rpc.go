package kclient

import "github.com/mohsanabbas/kproxy/internal/kwire"

// Metadata fetches MetadataResponse v9 (first flexible). Topics may be nil to
// request the full cluster metadata.
func (c *Conn) Metadata(topics []string, allowAutoCreate bool) (kwire.MetadataResponse, error) {
	const ver int16 = 9
	body := encodeMetadataReqV9(topics, allowAutoCreate)
	resp, err := c.Do(kwire.APIMetadata, ver, body)
	if err != nil {
		return kwire.MetadataResponse{}, err
	}
	return kwire.DecodeMetadataResponse(resp, ver)
}

// encodeMetadataReqV9 builds a MetadataRequest v9 body. v9 schema:
//
//	Topics: COMPACT_NULLABLE_ARRAY of {TopicID UUID(v10+ only) | Name COMPACT_STRING}
//	AllowAutoTopicCreation: BOOL (v4+)
//	IncludeClusterAuthorizedOperations: BOOL (v8-10)
//	IncludeTopicAuthorizedOperations: BOOL (v8+)
//	tagged fields
func encodeMetadataReqV9(topics []string, allowAutoCreate bool) []byte {
	var dst []byte
	if topics == nil {
		dst = kwire.AppendUvarint(dst, 0) // null
	} else {
		// Topic count is supplied by kproxy itself (cache refresh path) it is
		// bounded by the cluster size, never near uint32 limits.
		dst = kwire.AppendUvarint(dst, uint32(len(topics)+1)) // #nosec G115 -- bounded by cluster topic count
		for _, t := range topics {
			dst = kwire.AppendCompactString(dst, t)
			dst = kwire.AppendEmptyTaggedFields(dst) // per-topic tagged fields
		}
	}
	if allowAutoCreate {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = append(dst,
		0, // includeClusterAuthorizedOperations
		0, // includeTopicAuthorizedOperations
	)
	dst = kwire.AppendEmptyTaggedFields(dst)
	return dst
}

// ListOffsets issues a ListOffsets v6 RPC.
func (c *Conn) ListOffsets(req kwire.ListOffsetsRequest) (kwire.ListOffsetsResponse, error) {
	body := kwire.AppendListOffsetsRequest(nil, req)
	resp, err := c.Do(kwire.APIListOffsets, kwire.ListOffsetsVersion, body)
	if err != nil {
		return kwire.ListOffsetsResponse{}, err
	}
	return kwire.DecodeListOffsetsResponse(resp)
}

// OffsetFetch issues an OffsetFetch v8 RPC.
func (c *Conn) OffsetFetch(req kwire.OffsetFetchRequest) (kwire.OffsetFetchResponse, error) {
	body := kwire.AppendOffsetFetchRequest(nil, req)
	resp, err := c.Do(kwire.APIOffsetFetch, kwire.OffsetFetchVersion, body)
	if err != nil {
		return kwire.OffsetFetchResponse{}, err
	}
	return kwire.DecodeOffsetFetchResponse(resp)
}

// DescribeGroups issues a DescribeGroups v5 RPC.
func (c *Conn) DescribeGroups(req kwire.DescribeGroupsRequest) (kwire.DescribeGroupsResponse, error) {
	body := kwire.AppendDescribeGroupsRequest(nil, req)
	resp, err := c.Do(kwire.APIDescribeGroups, kwire.DescribeGroupsVersion, body)
	if err != nil {
		return kwire.DescribeGroupsResponse{}, err
	}
	return kwire.DecodeDescribeGroupsResponse(resp)
}
