-- The host's side of the demo: share the project this Neovim was started in, hand the page
-- link on through a file, and stay in the room.
--
-- `SELVAGE_DEMO_LINK_FILE` is where the link goes; the demo's other windows wait on it. With
-- `SELVAGE_DEMO_TYPE` set, the host types it into the shared file one character at a time,
-- which is what the recorded demo shows and what a person does by hand otherwise.
--
-- Nothing here waits on the event loop: a `vim.wait` loop is a Neovim with its messages held
-- back, and a message that arrives during one is a hit-enter prompt the demo would have to
-- answer. Every wait is a timer instead.

local link_file = assert(vim.env.SELVAGE_DEMO_LINK_FILE, 'SELVAGE_DEMO_LINK_FILE is not set')
local server = assert(vim.env.SELVAGE_DEMO_SERVER, 'SELVAGE_DEMO_SERVER is not set')
local name = vim.env.SELVAGE_DEMO_NAME or 'Ada'
local shared = vim.env.SELVAGE_DEMO_SHARED or 'src/limit.lua'

package.path = (vim.env.SELVAGE_DEMO_SCENE_DIR or '.') .. '/?.lua;' .. package.path
local typing = require('typing')

vim.g.selvage_display_name = name
vim.opt.number = true
vim.opt.signcolumn = 'yes'
vim.opt.swapfile = false

local selvage = require('selvage')

vim.cmd('edit ' .. vim.fn.fnameescape(shared))
selvage.host(server)

-- The link a person opens is the page the server serves, carrying the room's token and keys:
-- a host holds the wire link, so the page link is asked for the way the command asks for it.
local function hand_on_invite(attempt)
  attempt = attempt or 0
  if attempt > 400 then
    error('the host minted no room within 40s')
  end
  local session = selvage.session()
  if session.status == 'failed' then
    error('the host could not open a room: ' .. vim.inspect(session))
  end
  if session.invite == nil then
    vim.defer_fn(function()
      hand_on_invite(attempt + 1)
    end, 100)
    return
  end
  local link = vim.fn.getreg('"')
  if not link:match('^https?://') then
    selvage.copy_invite()
    link = vim.fn.getreg('"')
  end
  if not link:match('^https?://') then
    error('the host holds no page link to hand on')
  end
  local handle = assert(io.open(link_file, 'w'))
  handle:write(link)
  handle:close()

  if vim.env.SELVAGE_DEMO_TYPE then
    local delay = tonumber(vim.env.SELVAGE_DEMO_TYPE_DELAY or '6000') or 6000
    vim.defer_fn(function()
      local buffer = vim.api.nvim_get_current_buf()
      local row = vim.fn.search('^M.limits', 'w')
      if row == 0 then
        row = 3
      end
      vim.api.nvim_win_set_cursor(0, { row, 0 })
      local line = vim.api.nvim_buf_get_lines(buffer, row - 1, row, false)[1]
      typing.after(buffer, row - 1, #line, '\n' .. vim.env.SELVAGE_DEMO_TYPE, 70)
    end, delay)
  end
end

hand_on_invite()
