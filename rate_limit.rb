module RateLimit
  class Counter
    def initialize(cache, key, resolution=60, timestamp=Time.now.to_i)
      @cache      = cache
      @key        = namespace(key)
      @resolution = resolution
      @timestamp  = timestamp
    end

    def reached?(limit=nil, buckets=5)
      return false if limit.nil?

      limit <= (
        @cache.read_multi(
          *buckets.times.map { |i| bucket(@timestamp - (i * @resolution)) }
        ).
        reduce(0) { |total,(_,count)| total += count.to_i }
      )
    end

    def increment(offset=1)
      key = bucket(@timestamp)

      if @cache.increment(key, offset).nil?
        @cache.write(key, offset)
      end
    end

    protected

    def namespace(key)
      "ratelimit-#{key}"
    end

    private

    def bucket(timestamp)
      "#{@key}-#{timestamp - (timestamp % @resolution)}"
    end
  end
end

if __FILE__ == $0
  require 'test/unit'

  class RateLimitTest < Test::Unit::TestCase
    include RateLimit

    class HashCache < Hash
      def read_multi(*names)
        names.inject({}) { |memo,k| memo[k] = self[k]; memo }
      end

      def write(key, value)
        self[key] = value
      end

      def increment(key, offset)
        self[key] = self[key] && self[key] + offset
      end
    end

    def setup
      @cache      = HashCache.new
      @resolution = 60
      @timestamp  = Time.mktime(2011, 11, 5, 0, 0).to_i
      @counter    = Counter.new(@cache, 'api-123', @resolution, @timestamp)
    end

    def teardown
      @cache.clear
    end

    def test_limit_reached_without_limit
      assert_equal(false, @counter.reached?)
    end

    def test_limit_reached_on_empty_cache
      assert_equal(false, @counter.reached?(10))
    end

    def test_log_on_empty_cache_creates_counter
      10.times { |i| @counter.increment }
      assert_equal(1, @cache.keys.size)
      assert_equal(10, @cache[@cache.keys.first])
    end

    def test_1_second_update_resolution
      60.times { |i| Counter.new(@cache, 'key', 1, @timestamp - (i + 1)).increment }
      assert_equal(60, @cache.keys.size)
      assert_equal([ 1 ], @cache.values.uniq)
    end

    def test_30_seconds_resolution
      60.times { |i| Counter.new(@cache, 'key', 30, @timestamp - (i + 1)).increment }
      assert_equal(2, @cache.keys.size)
      assert_equal([ 30, 30 ], @cache.values)
    end

    def test_60_seconds_resolution
      60.times { |i| @counter.increment }
      assert_equal(1, @cache.keys.size)
      assert_equal([ 60 ], @cache.values)
    end

    def test_limit_reached_on_less_than_limit_logged
      9.times { |i| @counter.increment }
      assert_equal(false, @counter.reached?(10))
    end

    def test_limit_reached
      10.times { |i| @counter.increment }
      assert_equal(true, @counter.reached?(10))
    end

    def test_higher_increments
      10.times { |i| @counter.increment(10) }
      assert_equal(true, @counter.reached?(100))
    end

    def test_slot_limits
      c1 = Counter.new(@cache, 'key', 300, @timestamp - 301)
      c2 = Counter.new(@cache, 'key', 300, @timestamp - 300)
      c3 = Counter.new(@cache, 'key', 300, @timestamp - 240)
      c4 = Counter.new(@cache, 'key', 300, @timestamp)
      [ c1, c2, c3, c4 ].each(&:increment)
      assert_equal(true,  c4.reached?(1, 1))
      assert_equal(false, c4.reached?(2, 1))
      assert_equal(true,  c4.reached?(3, 2))
      assert_equal(false, c4.reached?(4, 2))
      assert_equal(true,  c4.reached?(4, 3))
      assert_equal(true,  c4.reached?(4))
      assert_equal(false, c4.reached?(5))
    end

    def test_multiple_guards_dont_interfere
      c2 = Counter.new(@cache, 'key', @resolution, @timestamp)
      10.times { @counter.increment ; c2.increment }
      10.times { c2.increment }
      assert_equal(true, @counter.reached?(10))
      assert_equal(false, @counter.reached?(11))
      assert_equal(true, c2.reached?(20))
      assert_equal(false, c2.reached?(21))
      assert_equal(@cache.inject(0) { |m,(k,v)| m += v }, 30)
    end
  end
end
