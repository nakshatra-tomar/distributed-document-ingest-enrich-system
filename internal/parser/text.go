package parser

import (
    "fmt"
    "strings"
    "unicode/utf8"
)

// TextParser implements basic normalization for plain-text files.
type TextParser struct{}

// NewTextParser returns a new instance of TextParser.
func NewTextParser() *TextParser {
    return &TextParser{}
}

// Parse ensures the content is valid UTF-8, normalizes line endings,
// trims whitespace, and removes blank lines.
func (p *TextParser) Parse(content []byte) (string, error) {
    // Validate UTF-8 integrity
    if !utf8.Valid(content) {
        return "", fmt.Errorf("content is not valid UTF-8")
    }

    text := string(content)

    // Normalize CRLF and CR line endings to LF
    text = strings.ReplaceAll(text, "\r\n", "\n")
    text = strings.ReplaceAll(text, "\r", "\n")
    // Reason: Windows uses CRLF; old Macs use CR. Standardizing on LF ensures consistency. :contentReference[oaicite:1]{index=1}

    // Split lines, trim whitespace, and skip blank lines
    lines := strings.Split(text, "\n")
    var cleanLines []string
    for _, line := range lines {
        trimmed := strings.TrimSpace(line)
        if trimmed != "" {
            cleanLines = append(cleanLines, trimmed)
        }
    }
    // Dropping blank lines ensures compacted output. Iterative trimming is idiomatic over regex or Replace. :contentReference[oaicite:2]{index=2}

    return strings.Join(cleanLines, "\n"), nil
}

// SupportedTypes returns the file types this parser supports.
func (p *TextParser) SupportedTypes() []string {
    return []string{"txt", "log", "text"}
}
