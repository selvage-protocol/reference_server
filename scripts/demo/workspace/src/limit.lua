local M = {}

M.limits = { doing = 2 }

function M.room(name, board)
  return #board:column(name) < (M.limits[name] or math.huge)
end

return M
