file = File.open("schema.txt", "r")

lines = []
while (line = file.gets)
  lines << line.chomp
end

puts lines.join("")