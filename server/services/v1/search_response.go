package v1

import tsApi "github.com/typesense/typesense-go/typesense/api"

type SearchResponse struct {
	Hits   *HitsResponse
	Facets *FacetResponse
}

type HitsResponse struct {
	Hits *[]tsApi.SearchResultHit
}

func NewHitsResponse() *HitsResponse {
	var hits []tsApi.SearchResultHit
	return &HitsResponse{
		Hits: &hits,
	}
}

func (h *HitsResponse) Append(hits *[]tsApi.SearchResultHit) {
	if hits != nil {
		*h.Hits = append(*h.Hits, *hits...)
	}
}

func (h *HitsResponse) GetDocument(idx int) (*map[string]interface{}, bool) {
	if idx < len(*h.Hits) {
		return (*h.Hits)[idx].Document, true
	}

	return nil, false
}

func (h *HitsResponse) HasMoreHits(idx int) bool {
	return idx < len(*h.Hits)
}

type FacetResponse struct {
	Facets *[]tsApi.FacetCounts
}

func CreateFacetResponse(facets *[]tsApi.FacetCounts) *FacetResponse {
	if facets != nil {
		return &FacetResponse{
			Facets: facets,
		}
	}

	return nil
}
