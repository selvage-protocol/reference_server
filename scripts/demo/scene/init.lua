-- The editor the demo video draws in: a clean Neovim, so what a recording shows is the plugin
-- rather than somebody's own configuration.
vim.opt.termguicolors = true
vim.opt.number = true
vim.opt.signcolumn = 'yes'
vim.opt.swapfile = false
vim.opt.undofile = false
vim.opt.laststatus = 2
vim.opt.tabstop = 2
vim.opt.shiftwidth = 2
vim.opt.expandtab = true
vim.opt.cursorline = true
vim.opt.fillchars = { eob = ' ' }
vim.opt.cmdheight = 3
vim.opt.more = false
-- A message that outgrows the command line is a hit-enter prompt, and a prompt is a blocked
-- event loop: the room would stop drawing while it waits for a key nobody presses. One line
-- is kept, and the rest is in `:messages`.
local echo = vim.api.nvim_echo
vim.notify = function(message)
  local line = tostring(message):match('^[^\n]*') or ''
  if #line > 56 then
    line = line:sub(1, 53) .. '...'
  end
  echo({ { line } }, false, {})
end
vim.opt.shortmess:append('F')
-- No system clipboard in a headless demo host: an in-process provider keeps the plugin's own
-- copy step from asking for one, so the room's link lands in the register it was meant for.
vim.g.clipboard = {
  name = 'selvage-demo',
  copy = {
    ['+'] = function(lines) return #table.concat(lines, '\n') end,
    ['*'] = function(lines) return #table.concat(lines, '\n') end,
  },
  paste = { ['+'] = function() return { '' } end, ['*'] = function() return { '' } end },
}
vim.cmd.colorscheme('habamax')

-- The status line names the file the way the room does: a guest's buffer is the mirror's copy,
-- and the mirror's path is the machine's business rather than the video's.
_G.SelvageDemoPath = function()
  local name = vim.api.nvim_buf_get_name(0)
  if name == '' then
    return '[no name]'
  end
  return name:match('([^/]+/[^/]+)$') or vim.fn.fnamemodify(name, ':t')
end
vim.opt.statusline = '%{%v:lua.SelvageDemoPath()%}%m %=%l:%c'
