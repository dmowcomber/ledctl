package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	effects "github.com/Jon-Bright/ledctl/effects"
	pixarray "github.com/Jon-Bright/ledctl/pixarray"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTConfig struct {
	Broker          string
	User            string
	Password        string
	Topic           string // Base topic, default "ledctl/light"
	DiscoveryPrefix string // Discovery prefix, default "homeassistant"
	Name            string // HA light entity name, default "LED Strip"
	ClientID        string // Unique identifier & MQTT client ID, default "ledctl"
}

type MQTTClient struct {
	cfg    MQTTConfig
	client mqtt.Client
	server *Server
	mu     sync.Mutex
}

type HADiscoveryPayload struct {
	Name               string   `json:"name"`
	UniqueID           string   `json:"unique_id"`
	Schema             string   `json:"schema"`
	CommandTopic       string   `json:"command_topic"`
	StateTopic         string   `json:"state_topic"`
	AvailabilityTopic  string   `json:"availability_topic"`
	Brightness         bool     `json:"brightness"`
	SupportedColorMode []string `json:"supported_color_modes"`
	Effect             bool     `json:"effect"`
	EffectList         []string `json:"effect_list"`
	Device             HADevice `json:"device"`
}

type HADevice struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Model        string   `json:"model"`
	Manufacturer string   `json:"manufacturer"`
}

type HALightState struct {
	State      string      `json:"state"`
	Brightness *int        `json:"brightness,omitempty"`
	ColorMode  string      `json:"color_mode,omitempty"`
	Color      *HARGBColor `json:"color,omitempty"`
	Effect     string      `json:"effect,omitempty"`
}

type HARGBColor struct {
	R int `json:"r"`
	G int `json:"g"`
	B int `json:"b"`
}

type HALightCommand struct {
	State      *string     `json:"state,omitempty"`
	Brightness *int        `json:"brightness,omitempty"`
	Color      *HARGBColor `json:"color,omitempty"`
	Effect     *string     `json:"effect,omitempty"`
	Transition *float64    `json:"transition,omitempty"`
}

func NewMQTTClient(cfg MQTTConfig, server *Server) (*MQTTClient, error) {
	mc := &MQTTClient{
		cfg:    cfg,
		server: server,
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.Broker)
	if cfg.User != "" {
		opts.SetUsername(cfg.User)
	}
	if cfg.Password != "" {
		opts.SetPassword(cfg.Password)
	}
	opts.SetClientID(cfg.ClientID)
	opts.SetAutoReconnect(true)

	availTopic := fmt.Sprintf("%s/availability", cfg.Topic)
	opts.SetWill(availTopic, "offline", 1, true)

	opts.OnConnect = func(c mqtt.Client) {
		log.Printf("Connected to MQTT broker at %s", cfg.Broker)
		c.Publish(availTopic, 1, true, "online")

		discTopic := fmt.Sprintf("%s/light/%s/config", cfg.DiscoveryPrefix, cfg.ClientID)
		discPayload := HADiscoveryPayload{
			Name:               cfg.Name,
			UniqueID:           fmt.Sprintf("%s_light", cfg.ClientID),
			Schema:             "json",
			CommandTopic:       fmt.Sprintf("%s/set", cfg.Topic),
			StateTopic:         fmt.Sprintf("%s/state", cfg.Topic),
			AvailabilityTopic:  availTopic,
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
		data, err := json.Marshal(discPayload)
		if err == nil {
			c.Publish(discTopic, 1, true, data)
			log.Printf("Published HA MQTT discovery config to %s", discTopic)
		} else {
			log.Printf("Error marshaling HA discovery payload: %v", err)
		}

		cmdTopic := fmt.Sprintf("%s/set", cfg.Topic)
		token := c.Subscribe(cmdTopic, 1, mc.handleCommand)
		token.Wait()
		if token.Error() != nil {
			log.Printf("Error subscribing to MQTT command topic %s: %v", cmdTopic, token.Error())
		} else {
			log.Printf("Subscribed to MQTT command topic %s", cmdTopic)
		}

		mc.PublishState()
	}

	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		return nil, fmt.Errorf("MQTT connection error: %v", token.Error())
	}

	mc.client = client
	return mc, nil
}

func (mc *MQTTClient) PublishState() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.client == nil || !mc.client.IsConnected() {
		return
	}
	stateTopic := fmt.Sprintf("%s/state", mc.cfg.Topic)
	st := mc.server.getHALightState()
	data, err := json.Marshal(st)
	if err != nil {
		log.Printf("Error marshaling state JSON: %v", err)
		return
	}
	mc.client.Publish(stateTopic, 1, true, data)
}

func (mc *MQTTClient) handleCommand(c mqtt.Client, msg mqtt.Message) {
	log.Printf("MQTT command received on %s: %s", msg.Topic(), string(msg.Payload()))
	var cmd HALightCommand
	err := json.Unmarshal(msg.Payload(), &cmd)
	if err != nil {
		log.Printf("Error unmarshaling MQTT command JSON: %v", err)
		return
	}

	mc.server.processHACommand(cmd)
}

func (s *Server) getHALightState() HALightState {
	if s.off {
		return HALightState{State: "OFF"}
	}

	pixels := s.pa.GetPixels()
	p := pixels[0]
	maxVal := p.R
	if p.G > maxVal {
		maxVal = p.G
	}
	if p.B > maxVal {
		maxVal = p.B
	}
	maxChan := s.pa.MaxPerChannel()

	var brightness int
	var rgb HARGBColor
	if maxVal == 0 {
		brightness = 255
		rgb = HARGBColor{R: 255, G: 255, B: 255}
	} else {
		brightness = int(math.Round((float64(maxVal) / float64(maxChan)) * 255.0))
		rgb = HARGBColor{
			R: int(math.Round((float64(p.R) / float64(maxVal)) * 255.0)),
			G: int(math.Round((float64(p.G) / float64(maxVal)) * 255.0)),
			B: int(math.Round((float64(p.B) / float64(maxVal)) * 255.0)),
		}
	}

	st := HALightState{
		State:      "ON",
		Brightness: &brightness,
		ColorMode:  "rgb",
		Color:      &rgb,
	}

	if s.running && s.laste != nil {
		st.Effect = s.laste.Name()
	}

	return st
}

func (s *Server) computeTargetPixel(cmd HALightCommand) pixarray.Pixel {
	maxChan := float64(s.pa.MaxPerChannel())

	targetBrightness := 255
	if cmd.Brightness != nil {
		targetBrightness = *cmd.Brightness
		if targetBrightness < 0 {
			targetBrightness = 0
		}
		if targetBrightness > 255 {
			targetBrightness = 255
		}
	}

	var rNorm, gNorm, bNorm float64

	if cmd.Color != nil {
		rNorm = float64(cmd.Color.R) / 255.0
		gNorm = float64(cmd.Color.G) / 255.0
		bNorm = float64(cmd.Color.B) / 255.0
	} else {
		pix := s.pa.GetPixels()[0]
		maxVal := pix.R
		if pix.G > maxVal {
			maxVal = pix.G
		}
		if pix.B > maxVal {
			maxVal = pix.B
		}
		if maxVal == 0 {
			rNorm, gNorm, bNorm = 1.0, 1.0, 1.0
		} else {
			rNorm = float64(pix.R) / float64(maxVal)
			gNorm = float64(pix.G) / float64(maxVal)
			bNorm = float64(pix.B) / float64(maxVal)
		}
	}

	bFactor := float64(targetBrightness) / 255.0

	return pixarray.Pixel{
		R: int(math.Round(rNorm * bFactor * maxChan)),
		G: int(math.Round(gNorm * bFactor * maxChan)),
		B: int(math.Round(bNorm * bFactor * maxChan)),
		W: 0,
	}
}

func (s *Server) processHACommand(cmd HALightCommand) {
	if cmd.State != nil && strings.ToUpper(*cmd.State) == "OFF" {
		fb := effects.NewFade(20*time.Second, pixarray.Pixel{R: 0, G: 0, B: 0, W: 0})
		s.off = true
		s.c <- fb
		s.notifyStateChange()
		return
	}

	// State is "ON" or unspecified with effect/color/brightness
	s.off = false

	if cmd.Effect != nil {
		effName := strings.ToUpper(*cmd.Effect)
		var e effects.Effect
		switch effName {
		case "RAINBOW":
			d := 30 * time.Second
			if cmd.Transition != nil && *cmd.Transition > 0 {
				d = time.Duration(*cmd.Transition * float64(time.Second))
			}
			e = effects.NewRainbow(d)
		case "CYCLE":
			d := 300 * time.Second
			if cmd.Transition != nil && *cmd.Transition > 0 {
				d = time.Duration(*cmd.Transition * float64(time.Second))
			}
			e = effects.NewCycle(d)
		case "KNIGHTRIDER":
			d := 5 * time.Second
			if cmd.Transition != nil && *cmd.Transition > 0 {
				d = time.Duration(*cmd.Transition * float64(time.Second))
			}
			e = effects.NewKnightRider(d, s.pa.NumPixels()/4)
		case "ZIP_SET_ALL", "ZIP":
			d := 2 * time.Second
			if cmd.Transition != nil && *cmd.Transition > 0 {
				d = time.Duration(*cmd.Transition * float64(time.Second))
			}
			pix := s.computeTargetPixel(cmd)
			e = effects.NewZip(d, pix)
		case "FADE_ALL", "FADE":
			fallthrough
		default:
			d := 1 * time.Second
			if cmd.Transition != nil && *cmd.Transition > 0 {
				d = time.Duration(*cmd.Transition * float64(time.Second))
			}
			pix := s.computeTargetPixel(cmd)
			e = effects.NewFade(d, pix)
		}

		s.laste = e
		s.c <- e
		s.notifyStateChange()
		return
	}

	if cmd.Color != nil || cmd.Brightness != nil {
		d := 1 * time.Second
		if cmd.Transition != nil && *cmd.Transition > 0 {
			d = time.Duration(*cmd.Transition * float64(time.Second))
		}
		pix := s.computeTargetPixel(cmd)
		e := effects.NewFade(d, pix)
		s.laste = e
		s.c <- e
		s.notifyStateChange()
		return
	}

	// Plain "ON" command
	if s.laste != nil {
		s.c <- s.laste
	} else {
		d := 1 * time.Second
		if cmd.Transition != nil && *cmd.Transition > 0 {
			d = time.Duration(*cmd.Transition * float64(time.Second))
		}
		pix := s.computeTargetPixel(cmd)
		e := effects.NewFade(d, pix)
		s.laste = e
		s.c <- e
	}
	s.notifyStateChange()
}

func (s *Server) notifyStateChange() {
	if s.mqtt != nil {
		s.mqtt.PublishState()
	}
}
