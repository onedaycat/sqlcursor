package sqlcursor

import "errors"

var (
	// Badrequest Error
	ErrNoQuery                = errors.New("No query")
	ErrNoSort                 = errors.New("Cursor is required atleast one sort")
	ErrNoDataInToken          = errors.New("No data in token")
	ErrInsufficientTokenValue = errors.New("Size of value in token must less than number of sort(s)")

	// Internal error
	ErrUnableCreateNextToken = errors.New("Unable create next token")
)
