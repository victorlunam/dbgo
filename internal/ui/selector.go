package ui

import (
	"fmt"
	"io"
	"strings"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
)

type Item struct {
	Value    string
	Selected bool
}

func (i Item) FilterValue() string { return "" }

type keyMap struct {
	Toggle key.Binding
	Accept key.Binding
	Quit   key.Binding
	Help   key.Binding
}

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Toggle, k.Accept, k.Quit}
}

func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Toggle, k.Accept, k.Quit, k.Help},
	}
}

var keys = keyMap{
	Toggle: key.NewBinding(
		key.WithKeys("space", " "),
		key.WithHelp("space", "toggle selection"),
	),
	Accept: key.NewBinding(
		key.WithKeys("enter"),
		key.WithHelp("enter", "accept selection"),
	),
	Quit: key.NewBinding(
		key.WithKeys("q", "esc", "ctrl+c"),
		key.WithHelp("q/esc", "quit"),
	),
	Help: key.NewBinding(
		key.WithKeys("?"),
		key.WithHelp("?", "toggle help"),
	),
}

type itemDelegate struct{}

func (d itemDelegate) Height() int                             { return 1 }
func (d itemDelegate) Spacing() int                            { return 0 }
func (d itemDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }
func (d itemDelegate) Render(w io.Writer, m list.Model, index int, listItem list.Item) {
	i, ok := listItem.(Item)
	if !ok {
		return
	}

	style := itemStyle
	checkbox := "[ ]"
	if i.Selected {
		checkbox = "[x]"
		style = selectedItemStyle
	}

	str := fmt.Sprintf("%s %s", checkbox, i.Value)

	fn := style.Render
	if index == m.Index() {
		fn = func(s ...string) string {
			return selectedItemStyle.Render(strings.Join(s, " "))
		}
	}

	fmt.Fprint(w, fn(str))
}

type Model struct {
	List     list.Model
	keys     keyMap
	help     help.Model
	quitting bool
}

func (m Model) Init() tea.Cmd {
	return nil
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.List.SetWidth(msg.Width)
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keys.Quit), key.Matches(msg, m.keys.Accept):
			m.quitting = true
			return m, tea.Quit
		case key.Matches(msg, m.keys.Toggle):
			i, ok := m.List.SelectedItem().(Item)
			if ok {
				i.Selected = !i.Selected
				m.List.SetItem(m.List.Index(), i)
			}
			return m, nil
		}
	}

	var cmd tea.Cmd
	m.List, cmd = m.List.Update(msg)
	return m, cmd
}

func (m Model) View() string {
	if m.quitting {
		itemsList := m.List.Items()
		selected := []string{}

		for i := range itemsList {
			item, ok := itemsList[i].(Item)
			if ok && item.Selected {
				selected = append(selected, item.Value)
			}
		}
		if len(selected) == 0 {
			return "You didn't select anything.\n"
		}

		return fmt.Sprintf("You chose: %s\n", strings.Join(selected, ", "))
	}

	return appStyle.Render(
		fmt.Sprintf(
			"%s\n\n%s\n\n%s",
			titleStyle.Render("Object Type Selector"),
			m.List.View(),
			m.help.View(m.keys),
		),
	)
}

func NewSelectorModel(options []string) Model {
	items := []list.Item{}
	for _, option := range options {
		items = append(items, Item{Value: option, Selected: false})
	}

	l := list.New(items, itemDelegate{}, defaultWidth, listHeight)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(false)
	l.SetShowHelp(false)
	l.DisableQuitKeybindings()
	l.Styles.PaginationStyle = paginationStyle
	l.Styles.HelpStyle = helpStyle

	h := help.New()
	h.ShowAll = false

	m := Model{List: l, keys: keys, help: h}

	return m
}
