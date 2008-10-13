class Module
  def bool_reader *args
    args.each do |sym|
      define_method("#{sym}?") { instance_variable_get "@#{sym}" }
    end
  end
end
