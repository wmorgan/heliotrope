THRIFT = "../thrift/build2/bin/thrift"

task :thrift => %w(thrift/heliotrope.thrift) do |t|
  rm Dir["gen-rb/*"]
  sh "#{THRIFT} --gen rb #{t.prerequisites * ' '}"
end

task :test do |t|
  sh "ruby -Ilib -Igen-rb test/all.rb"
end

task :edit do |t|
  sh "$EDITOR lib/heliotrope.rb lib/heliotrope/*.rb lib/heliotrope/*/*.rb bin/* thrift/*.thrift test/*.rb"
end
