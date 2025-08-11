package parser

import (
    "fmt"
)

// PDFParser is a placeholder for PDF content extraction.
type PDFParser struct{}

// NewPDFParser returns a new instance of PDFParser.
func NewPDFParser() *PDFParser {
    return &PDFParser{}
}

// Parse currently validates PDF signature and returns a placeholder.
// TODO: Replace with real PDF-to-text extraction (e.g., using a library like unidoc or pdftotext).
func (p *PDFParser) Parse(content []byte) (string, error) {
    if len(content) < 4 || string(content[:4]) != "%PDF" {
        return "", fmt.Errorf("invalid PDF content: missing '%%PDF' signature")
    }

    size := len(content)
    placeholder := fmt.Sprintf(`[PDF Document: %d bytes]

*** PDF parsing not implemented yet ***
(Here is a placeholder. Replace this with real extracted text using a PDF parser.)`, size)

    return placeholder, nil
}

// SupportedTypes returns file types handled by PDFParser.
func (p *PDFParser) SupportedTypes() []string {
    return []string{"pdf"}
}
