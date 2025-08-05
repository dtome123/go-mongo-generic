package mongodb

import (
	"math"
)

// Pagination : struct
type Pagination struct {
	PageLimit   int64 `json:"page_limit"`
	Offset      int64 `json:"offset"`
	PageCurrent int64 `json:"page_current"`
	TotalPage   int64 `json:"total_page"`
	TotalRecord int64 `json:"total_record"`
}

// Init : set limit, page -> set offset
func (p *Pagination) Init(page, limit int64) {
	if page < 1 {
		page = 1
	}
	if limit > 100 {
		limit = 100
	}
	p.PageCurrent = page
	p.PageLimit = limit
	p.Offset = p.PageLimit * (p.PageCurrent - 1)
}

// SetTotalRecord : set total record
func (p *Pagination) SetTotalRecord(totalRecord int64) {
	p.TotalRecord = totalRecord
	p.TotalPage = int64(math.Ceil(float64(p.TotalRecord) / float64(p.PageLimit)))
}
