package function

import (
	"bytes"
	"strings"
	"testing"
)

func TestProcessFileContent(t *testing.T) {
	var text = "cod_amm	cod_ou	cod_uni_ou	des_ou	regione	provincia	comune	indirizzo	cap	cf	dt_verifica_cf	data_avvio_sfe\nA	B	C	D	E	F	G	H	I	J	K	L"
	res := processFileContent(strings.NewReader(text))
	if len(res) != 1 {
		t.Error("Wrong outer length. Expected 1, actual", len(res))
	}
	if len(res[0]) != 2 {
		t.Error("Wrong inner length. Expected 2, actual", len(res[0]))
	}
	if res[0][0] != "C" {
		t.Error("Wrong first argument. Expected C, actual", res[0][0])
	}
	if res[0][1] != "J" {
		t.Error("Wrong second argument. Expected J, actual", res[0][1])
	}
}

func TestCreateCSV(t *testing.T) {
	content := [][]string{
		[]string{"C1", "J1"},
		[]string{"C2", "J2"},
	}
	b := new(bytes.Buffer)
	createCSV(b, content)
	stringContent := b.String()
	expected := "C1,J1\nC2,J2\n"
	if stringContent != expected {
		t.Error("Wrong result", stringContent)
	}
}
