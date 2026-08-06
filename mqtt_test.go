package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	effects "github.com/Jon-Bright/ledctl/effects"
	pixarray "github.com/Jon-Bright/ledctl/pixarray"
	rpi "github.com/Jon-Bright/ledctl/rpi"
)

type testLeds struct {
	pixels []pixarray.Pixel
}

func (l *testLeds) GetPixel(i int) pixarray.Pixel {
	return l.pixels[i]
}

func (l *testLeds) SetPixel(i int, p pixarray.Pixel) {
	l.pixels[i] = p
}

func (l *testLeds) Write() error {
	return nil
}

func (l *testLeds) MaxPerChannel() int {
	return 255
}

func (l *testLeds) RPi() *rpi.RPi {
	return nil
}

func createTestServer(numPixels int) *Server {
	leds := &testLeds{pixels: make([]pixarray.Pixel, numPixels)}
	pa := pixarray.NewPixArray(numPixels, 3, leds)
	c := make(chan effects.Effect, 10)
	return &Server{
		pa:      pa,
		c:       c,
		off:     true,
		running: false,
	}
}

func TestHADiscoveryPayload(t *testing.T) {
	cfg := MQTTConfig{
		Topic:           "ledctl/light",
		DiscoveryPrefix: "homeassistant",
		Name:            "Living Room LED",
		ClientID:        "ledctl_living",
	}

	payload := HADiscoveryPayload{
		Name:               cfg.Name,
		UniqueID:           cfg.ClientID + "_light",
		Schema:             "json",
		CommandTopic:       cfg.Topic + "/set",
		StateTopic:         cfg.Topic + "/state",
		AvailabilityTopic:  cfg.Topic + "/availability",
		Brightness:         true,
		SupportedColorMode: []string{"rgb"},
		Effect:             true,
		EffectList:         []string{"FADE_ALL", "ZIP_SET_ALL", "CYCLE", "RAINBOW", "KNIGHTRIDER"},
		Device: HADevice{
			Identifiers:  []string{cfg.ClientID},
			Name:         cfg.Name,
			Model:        "ledctl LED Controller",
			Manufacturer: "ledctl",
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal HA discovery payload: %v", err)
	}

	jsonStr := string(data)
	if !strings.Contains(jsonStr, `"schema":"json"`) {
		t.Errorf("Expected json schema in payload, got: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"command_topic":"ledctl/light/set"`) {
		t.Errorf("Expected command_topic in payload, got: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"unique_id":"ledctl_living_light"`) {
		t.Errorf("Expected unique_id in payload, got: %s", jsonStr)
	}
}

func TestGetHALightState(t *testing.T) {
	s := createTestServer(10)

	// Test 1: Server is OFF
	st := s.getHALightState()
	if st.State != "OFF" {
		t.Errorf("Expected state OFF, got %s", st.State)
	}

	// Test 2: Server is ON with red pixel at max 255
	s.off = false
	s.pa.SetAll(pixarray.Pixel{R: 255, G: 0, B: 0, W: 0})
	st = s.getHALightState()
	if st.State != "ON" {
		t.Errorf("Expected state ON, got %s", st.State)
	}
	if st.Brightness == nil || *st.Brightness != 255 {
		t.Errorf("Expected brightness 255, got %v", st.Brightness)
	}
	if st.Color == nil || st.Color.R != 255 || st.Color.G != 0 || st.Color.B != 0 {
		t.Errorf("Expected color R=255 G=0 B=0, got %v", st.Color)
	}

	// Test 3: Server running effect
	s.running = true
	s.laste = effects.NewRainbow(10 * time.Second)
	st = s.getHALightState()
	if st.Effect != "RAINBOW" {
		t.Errorf("Expected effect RAINBOW, got %s", st.Effect)
	}
}

func TestComputeTargetPixel(t *testing.T) {
	s := createTestServer(10)

	// Test RGB color + brightness 255
	bri := 255
	cmd := HALightCommand{
		Brightness: &bri,
		Color:      &HARGBColor{R: 255, G: 128, B: 0},
	}
	p := s.computeTargetPixel(cmd)
	if p.R != 255 || p.G != 128 || p.B != 0 {
		t.Errorf("Expected R=255 G=128 B=0, got %v", p)
	}

	// Test brightness 128 (50%)
	bri = 128
	cmd = HALightCommand{
		Brightness: &bri,
		Color:      &HARGBColor{R: 255, G: 0, B: 0},
	}
	p = s.computeTargetPixel(cmd)
	if p.R != 128 || p.G != 0 || p.B != 0 {
		t.Errorf("Expected R=128 G=0 B=0 for 50%% brightness, got %v", p)
	}
}

func TestProcessHACommand(t *testing.T) {
	s := createTestServer(10)

	// Test OFF command
	offState := "OFF"
	cmd := HALightCommand{State: &offState}
	s.processHACommand(cmd)

	if !s.off {
		t.Errorf("Expected s.off to be true after OFF command")
	}
	select {
	case eff := <-s.c:
		if eff.Name() != "FADE" {
			t.Errorf("Expected fade effect sent on OFF command, got %s", eff.Name())
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Expected effect in channel on OFF command")
	}

	// Test ON command with RAINBOW effect
	onState := "ON"
	rainbowEff := "RAINBOW"
	cmd = HALightCommand{State: &onState, Effect: &rainbowEff}
	s.processHACommand(cmd)

	if s.off {
		t.Errorf("Expected s.off to be false after ON command")
	}
	select {
	case eff := <-s.c:
		if eff.Name() != "RAINBOW" {
			t.Errorf("Expected RAINBOW effect sent, got %s", eff.Name())
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Expected effect in channel on RAINBOW command")
	}
}
