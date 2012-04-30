file = File.open("schema.txt", "r")

lines = []
while (line = file.gets)
  lines << line.chomp if !line.start_with? "--"
end

puts lines.join("")