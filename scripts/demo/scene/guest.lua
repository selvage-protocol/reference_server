-- The guest's side of the demo: wait for the host's page link, join the room, and edit the
-- file the room opened.
--
-- `SELVAGE_DEMO_LINK_FILE` is the file the host writes; `SELVAGE_DEMO_TYPE` is what this guest
-- types, one character at a time, when the demo is being recorded.
--
-- Every wait is a timer: a `vim.wait` loop holds messages back, and the join's own message is
-- one this window owes the reader.

local link_file = assert(vim.env.SELVAGE_DEMO_LINK_FILE, 'SELVAGE_DEMO_LINK_FILE is not set')
local name = vim.env.SELVAGE_DEMO_NAME or 'Grace'

package.path = (vim.env.SELVAGE_DEMO_SCENE_DIR or '.') .. '/?.lua;' .. package.path
local typing = require('typing')

vim.g.selvage_display_name = name
vim.opt.number = true
vim.opt.signcolumn = 'yes'
vim.opt.swapfile = false

local selvage = require('selvage')

local function wait_for_link(attempt)
  attempt = attempt or 0
  if attempt > 600 then
    error('the host wrote no invite within 60s')
  end
  local handle = io.open(link_file, 'r')
  if handle then
    local text = handle:read('*a')
    handle:close()
    if text ~= nil and text ~= '' then
      selvage.join(vim.trim(text))
      return
    end
  end
  vim.defer_fn(function()
    wait_for_link(attempt + 1)
  end, 100)
end

wait_for_link()

if vim.env.SELVAGE_DEMO_TYPE then
  local delay = tonumber(vim.env.SELVAGE_DEMO_TYPE_DELAY or '2500') or 2500
  vim.defer_fn(function()
    local buffer = vim.api.nvim_get_current_buf()
    local row = vim.fn.search('^return M', 'w')
    if row == 0 then
      row = vim.api.nvim_buf_line_count(buffer)
    end
    vim.api.nvim_win_set_cursor(0, { row, 0 })
    typing.after(buffer, row - 1, 0, vim.env.SELVAGE_DEMO_TYPE, 70)
  end, delay)
end
