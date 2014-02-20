# Carnivore NSQ

Provides NSQ `Carnivore::Source`

# Usage

```ruby
require 'carnivore'
require 'carnivore-nsq'

Carnivore.configure do
  source = Carnivore::Source.build(
    :type => :nsq,
    :args => {
    }
  )
end.start!
```

# Info
* Carnivore: https://github.com/carnivore-rb/carnivore
* Repository: https://github.com/carnivore-rb/carnivore-nsq
* IRC: Freenode @ #carnivore
