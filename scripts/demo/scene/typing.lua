-- Typing a snippet into a buffer one character at a time, so a recorded demo shows edits
-- arriving in the room the way a person's do rather than appearing whole.
--
-- The position is an extmark, so a character typed here lands where it was meant even while
-- the other seats' edits move the text around it.

local M = {}

local namespace = vim.api.nvim_create_namespace('selvage-demo-typing')

--- @param buffer integer
--- @param row integer 0-based row
--- @param column integer byte column
--- @param text string
--- @param interval_ms integer
function M.after(buffer, row, column, text, interval_ms)
  local mark = vim.api.nvim_buf_set_extmark(buffer, namespace, row, column, {})
  for index = 1, #text do
    local chunk = text:sub(index, index)
    vim.defer_fn(function()
      if not vim.api.nvim_buf_is_valid(buffer) then
        return
      end
      local position = vim.api.nvim_buf_get_extmark_by_id(buffer, namespace, mark, {})
      if position == nil or #position == 0 then
        return
      end
      local at_row, at_column = position[1], position[2]
      local lines = chunk == '\n' and { '', '' } or { chunk }
      vim.api.nvim_buf_set_text(buffer, at_row, at_column, at_row, at_column, lines)
      local win = vim.fn.bufwinid(buffer)
      if win ~= -1 then
        if chunk == '\n' then
          vim.api.nvim_win_set_cursor(win, { at_row + 2, 0 })
        else
          vim.api.nvim_win_set_cursor(win, { at_row + 1, at_column + #chunk })
        end
      end
    end, index * interval_ms)
  end
end

return M
