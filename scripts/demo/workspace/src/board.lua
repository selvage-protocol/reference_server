local Board = {}
Board.__index = Board

function Board.new()
  return setmetatable({ tasks = {}, next_id = 1 }, Board)
end

function Board:add(title, column)
  local task = { id = self.next_id, title = title, column = column }
  self.next_id = self.next_id + 1
  self.tasks[task.id] = task
  return task
end

function Board:move(id, to)
  local task = self.tasks[id]
  if task == nil then
    return nil, 'no task ' .. tostring(id)
  end
  task.column = to
  return task
end

function Board:column(name)
  local found = {}
  for _, task in pairs(self.tasks) do
    if task.column == name then
      found[#found + 1] = task
    end
  end
  table.sort(found, function(a, b) return a.id < b.id end)
  return found
end

return Board
