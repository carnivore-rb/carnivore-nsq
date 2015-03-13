require 'krakow'
require 'multi_json'

module Carnivore
  class Source
    class Nsq < Source

      trap_exit :trapper

      # @return [String] default path for global lookupd configuration
      DEFAULT_LOOKUPD_PATH = '/etc/carnivore/nsq.json'

      attr_reader(
        :lookupd, :http_transmit, :reader,
        :writer, :topic, :channel, :reader_args,
        :waiter, :producer_args, :args
      )

      # Setup the source
      #
      # @param args [Hash] setup arguments
      def setup(args={})
        @args = args.to_smash
        @lookupd = (default_lookupds + [args[:lookupd]]).flatten.compact.uniq
        @http_transmit = args[:http_transmit]
        @producer_args = args[:producer]
        @topic = args[:topic]
        @channel = args[:channel] || 'default'
        @reader_args = args[:reader_opts] || Smash.new
        @waiter = Celluloid::Condition.new
        Krakow::Utils::Logging.level = (Carnivore::Config.get(:krakow, :logging, :level) || :info).to_sym
      end

      # Recover from exit if unexpected or tear down conenctions if requested
      #
      # @param obj [Celluloid::Actor] failed actor
      # @param exception [Exception] actor exception
      def trapper(obj, exception)
        if(exception)
          exclusive do
            if(@reader && !@reader.alive?)
              warn 'Consumer failure detected. Rebuilding.'
              @reader = nil
              build_consumer
              info "Consumer connection for #{topic}:#{channel} re-established #{reader}"
            end
            unless(@writer.alive?)
              warn 'Producer failure detected. Rebuilding.'
              @writer = nil
              build_producer
              info "Producer connection for #{topic} re-established #{writer}"
            end
          end
        else
          @writer.terminate if @writer.alive?
          @reader.terminate if @reader.alive?
        end
      end

      # Clean up custom actors on teardown
      def teardown_cleanup
        [reader, writer].each do |r_con|
          if(r_con && r_con.alive?)
            begin
              r_con.terminate
            rescue Celluloid::Task::TerminatedError
              warn 'Terminated task error when cleaning NSQ connections. Moving on.'
            end
          end
        end
        super
      end

      # Build the consumer connection
      def build_consumer
        consumer_args = Smash.new(
          :nsqlookupd => lookupd,
          :topic => topic,
          :channel => channel,
          :max_in_flight => args.fetch(:max_in_flight, 100)
        ).merge(reader_args)
        @reader = Krakow::Consumer.new(consumer_args)
        link @reader
      end

      # Build the producer connection
      def build_producer
        if(producer_args)
          @writer = Krakow::Producer.new(
            producer_args.merge(
              Smash.new(
                :topic => topic
              )
            )
          )
          info "Producer TCP connection for #{topic} established #{writer}"
          link @writer
        elsif(http_transmit)
          @writer = Krakow::Producer::Http.new(
            Smash.new(
              :endpoint => http_transmit,
              :topic => topic
            )
          )
          info "Producer HTTP connection for #{topic} established #{writer}"
          link @writer
        end
      end

      # Establish required connections (producer/consumer)
      def connect
        unless(callbacks.empty?)
          unless(lookupd.empty?)
            build_consumer
            info "Consumer connection for #{topic}:#{channel} established #{reader}"
          end
        end
        build_producer
      end

      # @return [Krakow::Consumer]
      def consumer
        reader ||
          abort('Consumer is not established. No setup information provided!')
      end

      # @return [Krakow::Consumer, Krakow::Consumer::Http]
      def producer
        writer ||
          abort('Producer is not established. No setup information provided!')
      end

      # Receive messages
      #
      # @return [String]
      def receive(*_)
        msg = Celluloid::Future.new{ consumer.queue.pop }.value
        begin
          content = MultiJson.load(msg.message)
        rescue MultiJson::ParseError
          content = msg.message
        end
        {:raw => msg, :content => content}
      end

      # Send message
      #
      # @param payload [Object]
      # @param original [Carnivore::Message
      def transmit(payload, original=nil)
        payload = MultiJson.dump(payload) unless payload.is_a?(String)
        producer.write(payload)
      end

      # Confirm completion of message
      #
      # @param message [Carnivore::Message]
      # @return [TrueClass, FalseClass]
      def confirm(message)
        begin
          unless(consumer.confirm(message[:message]))
            error "Failed to confirm payload from source! (#{e})"
            false
          else
            true
          end
        rescue Krakow::Error::LookupFailed => e
          error "Failed to confirm payload from source! (#{e})"
          false
        end
      end

      # Touch message to extend lifetime
      #
      # @param message [Carnivore::Message]
      # @return [TrueClass]
      def touch(message)
        begin
          message[:message].touch
          true
        rescue Krakow::Error::LookupFailed => e
          error "Failed to touch payload from source! (#{e})"
          false
        end
      end

      private

      # @return [Array<String>] default lookupd locations
      def default_lookupds
        json_path = args.fetch(:lookupd_file_path, DEFAULT_LOOKUPD_PATH)
        lookupds = nil
        if(File.exists?(json_path))
          begin
            lookupds = MultiJson.load(
              File.read(json_path)
            ).to_smash[:lookupds]
          rescue MultiJson::LoadError => e
            error "Failed to load nsqlookupd file from system: #{e}"
          end
        end
        lookupds || []
      end

    end
  end
end
