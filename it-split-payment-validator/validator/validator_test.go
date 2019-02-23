package function

import (
	"fmt"
	"net/http"
	"os"
	"testing"
)

var ignored = os.Setenv("TESTING", "1")

var testDb = map[string]string{
	"SUCCESS": "ABCD",
}

func TestSuccessfulResponse(t *testing.T) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost?%s=SUCCESS", addresseeCodeParam), nil)
	if err != nil {
		t.Error("cannot create mock request", err)
	}
	res, err := performValidation(req, testDb)
	if err != nil {
		t.Error("expected success, got error", err)
	}
	expected := `{"success": true, "fiscalCode": "ABCD"}`
	if res != expected {
		t.Errorf("comparison failed. Expected %s, got %s", expected, res)
	}
}

func TestFailureResponse(t *testing.T) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost?%s=FAILURE", addresseeCodeParam), nil)
	if err != nil {
		t.Error("cannot create mock request", err)
	}
	res, err := performValidation(req, testDb)
	if err != nil {
		t.Error("expected success, got error", err)
	}
	expected := `{"success": false}`
	if res != expected {
		t.Errorf("comparison failed. Expected %s, got %s", expected, res)
	}
}

func TestMissingParameter(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost", nil)
	if err != nil {
		t.Error("cannot create mock request", err)
	}
	res, err := performValidation(req, testDb)
	if err == nil {
		t.Error("expected failure, got success", res)
	}
}
