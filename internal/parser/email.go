package parser

import (
    "bufio"
    "bytes"
    "fmt"
    "mime"
    "strings"
)

// EmailParser extracts a few useful headers and the text body from .eml emails.
type EmailParser struct{}

// NewEmailParser returns a new EmailParser.
func NewEmailParser() *EmailParser { return &EmailParser{} }

// Parse splits RFC-5322-style messages into headers and body, capturing
// From/To/Subject/Date. It supports folded headers and normalizes line endings.
func (p *EmailParser) Parse(content []byte) (string, error) {
    // normalize CRLF/CR to LF so the scanner logic is consistent
    content = bytes.ReplaceAll(content, []byte("\r\n"), []byte("\n"))
    content = bytes.ReplaceAll(content, []byte("\r"), []byte("\n"))

    scanner := bufio.NewScanner(bytes.NewReader(content))
    // Increase the token buffer so long lines won’t error (default ~64KiB).
    buf := make([]byte, 64*1024)
    scanner.Buffer(buf, 2*1024*1024) // up to 2 MiB per line

    var (
        // We keep ALL headers in order to handle folding, then filter the ones we care about.
        rawHeaderLines []string
        bodyBuilder    strings.Builder
        inHeaders      = true
    )

    for scanner.Scan() {
        line := scanner.Text()

        if inHeaders {
            if line == "" { // blank line separates headers from body
                inHeaders = false
                continue
            }
            // Header folding: continuation lines begin with space or tab;
            // append to previous header line to reconstruct the full header value.
            if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
                if len(rawHeaderLines) > 0 {
                    rawHeaderLines[len(rawHeaderLines)-1] += " " + strings.TrimSpace(line)
                }
                continue
            }
            rawHeaderLines = append(rawHeaderLines, line)
            continue
        }

        // Body
        bodyBuilder.WriteString(line)
        bodyBuilder.WriteByte('\n')
    }
    if err := scanner.Err(); err != nil {
        return "", fmt.Errorf("error scanning email content: %w", err)
    }

    // Filter + normalize important headers (case-insensitive).
    // Also try to decode MIME "encoded-words" (e.g., =?UTF-8?Q?...?=)
    var hdrOut strings.Builder
    wd := mime.WordDecoder{}
    want := []string{"from", "to", "subject", "date"}

    for _, hline := range rawHeaderLines {
        // Split on first colon only
        idx := strings.IndexByte(hline, ':')
        if idx <= 0 {
            continue
        }
        name := strings.ToLower(strings.TrimSpace(hline[:idx]))
        val := strings.TrimSpace(hline[idx+1:])

        for _, w := range want {
            if name == w {
                if dec, err := wd.DecodeHeader(val); err == nil && dec != "" {
                    val = dec
                }
                hdrOut.WriteString(strings.Title(name)) // From/To/Subject/Date
                hdrOut.WriteString(": ")
                hdrOut.WriteString(val)
                hdrOut.WriteByte('\n')
                break
            }
        }
    }

    // Build result
    var result strings.Builder
    result.WriteString("=== EMAIL HEADERS ===\n")
    result.WriteString(hdrOut.String())
    result.WriteString("\n=== EMAIL BODY ===\n")
    result.WriteString(bodyBuilder.String())

    return result.String(), nil
}

// SupportedTypes returns the file extensions handled by EmailParser.
func (p *EmailParser) SupportedTypes() []string {
    return []string{"eml", "email"}
}
