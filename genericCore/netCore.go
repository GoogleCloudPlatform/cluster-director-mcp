// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package genericCore

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/net/html"
)

func ScrapeURL(url string, tableHeaderToSearch string) ([][]string, string, bool) {
	var tableStrings [][]string

	// 1. Fetch the page
	resp, err := http.Get(url)
	if err != nil {
		return tableStrings, fmt.Sprintf("Could not fetch URL: %v", err), false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return tableStrings, fmt.Sprintf("Could not fetch url, Status code error: %d %s", resp.StatusCode, resp.Status), false
	}

	// 2. Parse HTML
	doc, err := html.Parse(resp.Body)
	if err != nil {
		return tableStrings, fmt.Sprintf("Could not to parse HTML from URL: %v", err), false
	}

	// 3. Find the correct table
	//tableNode := findTableByHeader(doc, "Mnemonic")
	tableNode := findTableByHeader(doc, tableHeaderToSearch)
	if tableNode == nil {
		return tableStrings, fmt.Sprintf("Could not find table with header %s", tableHeaderToSearch), false
	}

	// Write to strings builder
	var sb strings.Builder
	writer := csv.NewWriter(&sb)
	defer writer.Flush()

	// 5. Parse and print the table data
	tableStrings = processTable(tableNode, writer)

	return tableStrings, sb.String(), true
}

// findTableByHeader walks the tree to find a <table> that contains a specific header text
func findTableByHeader(n *html.Node, targetHeader string) *html.Node {
	if n.Type == html.ElementNode && n.Data == "table" {
		// We found a table, now check if it has the header we want
		if tableHasHeader(n, targetHeader) {
			return n
		}
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		found := findTableByHeader(c, targetHeader)
		if found != nil {
			return found
		}
	}
	return nil
}

// tableHasHeader checks if a specific text exists inside the <th> tags of a table
func tableHasHeader(table *html.Node, targetText string) bool {
	// We need to do a quick search inside this table node for the text
	var found bool
	var search func(*html.Node)
	search = func(n *html.Node) {
		if found {
			return
		}
		if n.Type == html.ElementNode && n.Data == "th" {
			if strings.Contains(extractText(n), targetText) {
				found = true
				return
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			search(c)
		}
	}
	search(table)
	return found
}

// processTable extracts rows and writes them to CSV
func processTable(table *html.Node, writer *csv.Writer) [][]string {

	var returnArray [][]string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			// Found a row, extract cells
			var row []string
			// Look for both th (headers) and td (data)
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (c.Data == "td" || c.Data == "th") {
					text := extractText(c)
					row = append(row, text)
				}
			}
			if len(row) > 0 {
				writer.Write(row)
				returnArray = append(returnArray, row)
			}
		}
		// Continue recursion
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(table)
	return returnArray
}

// extractText gets all text content from a node and its children recursively
func extractText(n *html.Node) string {
	if n.Type == html.TextNode {
		return strings.TrimSpace(n.Data)
	}
	var result strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		result.WriteString(extractText(c) + " ")
	}
	return strings.Join(strings.Fields(result.String()), " ") // Clean up extra whitespace
}
