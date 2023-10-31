package main

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mongodb/mongo-tools/common/log"
)

type model struct {
	d *Driver
}

type tickMsg time.Time

var _ (tea.Model) = (*model)(nil)

func (m *model) Init() tea.Cmd {
	return tickCmd()
}

// Update implements tea.Model.
func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	log.Logvf(log.DebugLow, "received %v", msg)
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		}
	case tickMsg:
		return m, tickCmd()
	}
	return m, nil
}

// View implements tea.Model.
func (m *model) View() string {
	log.Logv(log.DebugLow, "refreshing")
	return m.d.stateString()
}

func tickCmd() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
