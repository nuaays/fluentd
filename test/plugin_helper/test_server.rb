require_relative '../helper'
require 'fluent/plugin_helper/server'
require 'fluent/plugin/base'
require 'timeout'

require 'serverengine'
require 'fileutils'

class ServerPluginHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :server
  end

  PORT = unused_port

  setup do
    @socket_manager_path = ServerEngine::SocketManager::Server.generate_path
    if @socket_manager_path.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
    @socket_manager_server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
    ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s

    @d = Dummy.new
    @d.start
    @d.after_start
  end

  teardown do
    @d.stopped? || @d.stop
    @d.before_shutdown? || @d.before_shutdown
    @d.shutdown? || @d.shutdown
    @d.after_shutdown? || @d.after_shutdown
    @d.closed? || @d.close
    @d.terminated? || @d.terminate

    @socket_manager_server.close
    if @socket_manager_server.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
  end

  sub_test_case 'plugin instance' do
    test 'can be instantiated to be able to create threads' do
      d = Dummy.new
      assert d.respond_to?(:_servers)
      assert d._servers.empty?

      assert d.respond_to?(:server_wait_until_start)
      assert d.respond_to?(:server_wait_until_stop)
      assert d.respond_to?(:server_create_connection)
      assert d.respond_to?(:server_create)
      assert d.respond_to?(:server_create_tcp)
      assert d.respond_to?(:server_create_udp)
      assert d.respond_to?(:server_create_tls)
    end

    test 'can be configured' do
      d = Dummy.new
      assert_nothing_raised do
        d.configure(config_element())
      end
      assert d.plugin_id
      assert d.log
    end
  end

  # run tests for tcp, udp, tls and unix
  sub_test_case '#server_create and #server_create_connection' do
    methods = {server_create: :server_create, server_create_connection: :server_create_connection}

    data(methods)
    test 'raise error if title is not specified or not a symbol' do |m|
      assert_raise(ArgumentError.new("BUG: title must be a symbol")) do
        @d.__send__(m, nil, PORT){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: title must be a symbol")) do
        @d.__send__(m, "", PORT){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: title must be a symbol")) do
        @d.__send__(m, "title", PORT){|x| x }
      end
      assert_nothing_raised do
        @d.__send__(m, :myserver, PORT){|x| x }
      end
    end

    data(methods)
    test 'raise error if port is not specified or not an integer' do |m|
      assert_raise(ArgumentError.new("BUG: port must be an integer")) do
        @d.__send__(m, :myserver, nil){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: port must be an integer")) do
        @d.__send__(m, :myserver, "1"){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: port must be an integer")) do
        @d.__send__(m, :myserver, 1.5){|x| x }
      end
      assert_nothing_raised do
        @d.__send__(m, :myserver, PORT){|x| x }
      end
    end

    data(methods)
    test 'raise error if block is not specified' do |m|
      assert_raise(ArgumentError) do
        @d.__send__(m, :myserver, PORT)
      end
      assert_nothing_raised do
        @d.__send__(m, :myserver, PORT){|x| x }
      end
    end

    data(methods)
    test 'creates tcp server, binds 0.0.0.0 in default' do |m|
      @d.__send__(m, :myserver, PORT){|x| x }

      assert_equal 1, @d._servers.size
      assert_equal :myserver, @d._servers.first.title
      assert_equal PORT, @d._servers.first.port

      assert_equal :tcp, @d._servers.first.proto
      assert_equal "0.0.0.0", @d._servers.first.bind

      assert{ @d._servers.first.server.is_a? Coolio::TCPServer }
      assert_equal "0.0.0.0", @d._servers.first.server.instance_eval{ @listen_socket }.addr[3]
    end

    data(methods)
    test 'creates tcp server if specified in proto' do |m|
      @d.__send__(m, :myserver, PORT, proto: :tcp){|x| x }

      assert_equal :tcp, @d._servers.first.proto
      assert{ @d._servers.first.server.is_a? Coolio::TCPServer }
    end

    # tests about "proto: :udp" is in #server_create

    data(methods)
    test 'creates tls server if specified in proto' do |m|
      # pend "not implemented yet"
    end

    data(methods)
    test 'creates unix server if specified in proto' do |m|
      # pend "not implemented yet"
    end

    data(methods)
    test 'raise error if unknown protocol specified' do |m|
      assert_raise(ArgumentError.new("BUG: invalid protocol name")) do
        @d.__send__(m, :myserver, PORT, proto: :quic){|x| x }
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp],
      # 'server_create tls' => [:server_create, :tls],
      # 'server_create unix' => [:server_create, :unix],
      'server_create_connection tcp' => [:server_create_connection, :tcp],
      # 'server_create_connection tcp' => [:server_create_connection, :tls],
      # 'server_create_connection tcp' => [:server_create_connection, :unix],
    )
    test 'raise error if udp options specified for tcp/tls/unix' do |(m, proto)|
      assert_raise ArgumentError do
        @d.__send__(m, :myserver, PORT, proto: proto, max_bytes: 128){|x| x }
      end
      assert_raise ArgumentError do
        @d.__send__(m, :myserver, PORT, proto: proto, flags: 1){|x| x }
      end
    end

    data(
      'server_create udp' => [:server_create, :udp],
    )
    test 'raise error if tcp/tls options specified for udp' do |(m, proto)|
      assert_raise(ArgumentError.new("BUG: linger_timeout is available for tcp/tls")) do
        @d.__send__(m, :myserver, PORT, proto: proto, linger_timeout: 1, max_bytes: 128){|x| x }
      end
    end

    data(
      'server_create udp' => [:server_create, :udp],
    )
    test 'raise error if tcp/tls/unix options specified for udp' do |(m, proto)|
      assert_raise(ArgumentError.new("BUG: backlog is available for tcp/tls")) do
        @d.__send__(m, :myserver, PORT, proto: proto, backlog: 500){|x| x }
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      # 'server_create unix' => [:server_create, :unix, {}],
      'server_create_connection tcp' => [:server_create_connection, :tcp, {}],
      # 'server_create_connection unix' => [:server_create_connection, :unix, {}],
    )
    test 'raise error if tls options specified for tcp/udp/unix' do |(m, proto, kwargs)|
      assert_raise(ArgumentError.new("BUG: certopts is available only for tls")) do
        @d.__send__(m, :myserver, PORT, proto: proto, certopts: {}, **kwargs){|x| x }
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      # 'server_create tls' => [:server_create, :tls, {}],
      'server_create_connection tcp' => [:server_create_connection, :tcp, {}],
      # 'server_create_connection tls' => [:server_create_connection, :tls, {}],
    )
    test 'can bind specified IPv4 address' do |(m, proto, kwargs)|
      @d.__send__(m, :myserver, PORT, proto: proto, bind: "127.0.0.1", **kwargs){|x| x }
      assert_equal "127.0.0.1", @d._servers.first.bind
      assert_equal "127.0.0.1", @d._servers.first.server.instance_eval{ instance_variable_defined?(:@listen_socket) ? @listen_socket : @_io }.addr[3]
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      # 'server_create tls' => [:server_create, :tls, {}],
      'server_create_connection tcp' => [:server_create_connection, :tcp, {}],
      # 'server_create_connection tls' => [:server_create_connection, :tls, {}],
    )
    test 'can bind specified IPv6 address' do |(m, proto, kwargs)| # if available
      omit "IPv6 unavailable here" unless ipv6_enabled?
      @d.__send__(m, :myserver, PORT, proto: proto, bind: "::1", **kwargs){|x| x }
      assert_equal "::1", @d._servers.first.bind
      assert_equal "::1", @d._servers.first.server.instance_eval{ instance_variable_defined?(:@listen_socket) ? @listen_socket : @_io }.addr[3]
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      # 'server_create tls' => [:server_create, :tls, {}],
      # 'server_create unix' => [:server_create, :unix, {}],
      'server_create_connection tcp' => [:server_create, :tcp, {}],
      # 'server_create_connection tls' => [:server_create, :tls, {}],
      # 'server_create_connection unix' => [:server_create, :unix, {}],
    )
    test 'can create 2 or more servers which share same bind address and port if shared option is true' do |(m, proto, kwargs)|
      begin
        d2 = Dummy.new; d2.start; d2.after_start

        assert_nothing_raised do
          @d.__send__(m, :myserver, PORT, proto: proto, **kwargs){|x| x }
          d2.__send__(m, :myserver, PORT, proto: proto, **kwargs){|x| x }
        end
      ensure
        d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      # 'server_create tls' => [:server_create, :tls, {}],
      # 'server_create unix' => [:server_create, :unix, {}],
      'server_create_connection tcp' => [:server_create, :tcp, {}],
      # 'server_create_connection tls' => [:server_create, :tls, {}],
      # 'server_create_connection unix' => [:server_create, :unix, {}],
    )
    test 'cannot create 2 or more servers using same bind address and port if shared option is false' do |(m, proto, kwargs)|
      begin
        d2 = Dummy.new; d2.start; d2.after_start

        assert_nothing_raised do
          @d.__send__(m, :myserver, PORT, proto: proto, shared: false, **kwargs){|x| x }
        end
        assert_raise(Errno::EADDRINUSE) do
          d2.__send__(m, :myserver, PORT, proto: proto, **kwargs){|x| x }
        end
      ensure
        d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
      end
    end
  end

  sub_test_case '#server_create' do
    data(
      'tcp' => [:tcp, {}],
      'udp' => [:udp, {max_bytes: 128}],
      # 'tls' => [:tls, {}],
      # 'unix' => [:unix, {}],
    )
    test 'raise error if block argument is not specified or too many' do |(proto, kwargs)|
      assert_raise(ArgumentError.new("BUG: block must have 1 or 2 arguments")) do
        @d.server_create(:myserver, PORT, proto: proto, **kwargs){ 1 }
      end
      assert_raise(ArgumentError.new("BUG: block must have 1 or 2 arguments")) do
        @d.server_create(:myserver, PORT, proto: proto, **kwargs){|sock, conn, what_is_this| 1 }
      end
    end

    test 'creates udp server if specified in proto' do
      @d.server_create(:myserver, PORT, proto: :udp, max_bytes: 512){|x| x }

      assert_equal :udp, @d._servers.first.proto
      assert{ @d._servers.first.server.is_a? Fluent::PluginHelper::Server::EventHandler::UDPServer }
    end
  end

  sub_test_case '#server_create_tcp' do
    test 'can accept all keyword arguments valid for tcp server' do
      assert_nothing_raised do
        @d.server_create_tcp(:s, PORT, bind: '127.0.0.1', shared: false, resolve_name: true, linger_timeout: 10, backlog: 500) do |data, conn|
          # ...
        end
      end
    end

    test 'creates a tcp server just to read data' do
      received = ""
      @d.server_create_tcp(:s, PORT) do |data|
        received << data
      end
      3.times do
        sock = TCPSocket.new("127.0.0.1", PORT)
        sock.puts "yay"
        sock.puts "foo"
        sock.close
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
    end

    test 'creates a tcp server to read and write data' do
      received = ""
      responses = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        received << data
        conn.write "ack\n"
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.puts "yay"
          sock.puts "foo"
          responses << sock.readline
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'creates a tcp server to read and write data using IPv6' do
      omit "IPv6 unavailable here" unless ipv6_enabled?

      received = ""
      responses = []
      @d.server_create_tcp(:s, PORT, bind: "::1") do |data, conn|
        received << data
        conn.write "ack\n"
      end
      3.times do
        TCPSocket.open("::1", PORT) do |sock|
          sock.puts "yay"
          sock.puts "foo"
          responses << sock.readline
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'does not resolve name of client address in default' do
      received = ""
      sources = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        received << data
        sources << conn.remote_host
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s == "127.0.0.1" } }
    end

    test 'does resolve name of client address if resolve_name is true' do
      received = ""
      sources = []
      @d.server_create_tcp(:s, PORT, resolve_name: true) do |data, conn|
        received << data
        sources << conn.remote_host
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s != "127.0.0.1" && Socket.getaddrinfo(s, PORT, Socket::AF_INET).any?{|i| i[3] == "127.0.0.1"} } }
    end

    test 'can keep connections alive for tcp if keepalive specified' do
      pend "not implemented yet"
    end

    test 'raises error if plugin registers data callback for connection object from #server_create' do
      received = ""
      errors = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        received << data
        begin
          conn.data{|d| received << d.upcase }
        rescue => e
          errors << e
        end
      end
      TCPSocket.open("127.0.0.1", PORT) do |sock|
        sock.puts "foo"
      end
      waiting(10){ sleep 0.1 until received.bytesize == 4 || errors.size == 1 }
      assert_equal "foo\n", received
      assert_equal 1, errors.size
      assert_equal "data callback can be registered just once, but registered twice", errors.first.message
    end

    test 'can call write_complete callback if registered' do
      buffer = ""
      lines = []
      responses = []
      response_completes = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        conn.on(:write_complete){|c| response_completes << true }
        buffer << data
        if idx = buffer.index("\n")
          lines << buffer.slice!(0,idx+1)
          conn.write "ack\n"
        end
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.write "yay"
          sock.write "foo\n"
          begin
            responses << sock.readline
          rescue EOFError, IOError, Errno::ECONNRESET
            # ignore
          end
          sock.close
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 && response_completes.size == 3 }
      assert_equal ["yayfoo\n", "yayfoo\n", "yayfoo\n"], lines
      assert_equal ["ack\n","ack\n","ack\n"], responses
      assert_equal [true, true, true], response_completes
    end

    test 'can call before_close callback to send data if registered' do
      buffer = ""
      lines = []
      responses = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        conn.on(:before_close){|c| c.write "closing\n" }
        buffer << data
        if idx = buffer.index("\n")
          lines << buffer.slice!(0,idx+1)
          conn.write "ack\n"
          conn.close
        end
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.write "yay"
          sock.write "foo\n"
          begin
            responses << sock.readline
            responses << sock.readline
          rescue EOFError, IOError, Errno::ECONNRESET
            p(here: "rescue in client", error: $!)
            # ignore
          end
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 && responses.size == 6 }
      assert_equal ["yayfoo\n", "yayfoo\n", "yayfoo\n"], lines
      assert_equal ["ack\n","closing\n","ack\n","closing\n","ack\n","closing\n"], responses
    end

    test 'can call close callback if registered' do
      buffer = ""
      lines = []
      callback_results = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        conn.on(:close){|c| callback_results << "closed" }
        buffer << data
        if idx = buffer.index("\n")
          lines << buffer.slice!(0,idx+1)
          conn.write "ack\n"
        end
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.write "yay"
          sock.write "foo\n"
          begin
            while line = sock.readline
              if line == "ack\n"
                sock.close
              end
            end
          rescue EOFError, IOError, Errno::ECONNRESET
            # ignore
          end
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 && callback_results.size == 3 }
      assert_equal ["yayfoo\n", "yayfoo\n", "yayfoo\n"], lines
      assert_equal ["closed", "closed", "closed"], callback_results
    end
  end

  sub_test_case '#server_create_udp' do
    test 'can accept all keyword arguments valid for udp server'
    test 'creates a udp server just to read data'
    test 'creates a udp server to read and write data'
    test 'creates a udp server to read and write data using IPv6' do
      omit "IPv6 unavailable here" unless ipv6_enabled?
    end

    test 'does not resolve name of client address in default'
    test 'does resolve name of client address if resolve_name is true'

    test 'raises error if plugin registers data callback for connection object from #server_create'
    test 'can call write_complete callback if registered'
    test 'raises error if plugin registers before_close callback for udp'
    test 'raises error if plugin registers close callback for udp'
  end

  sub_test_case '#server_create_tls' do
    # not implemented yet

    # test 'can accept all keyword arguments valid for tcp/tls server'
    # test 'creates a tls server just to read data'
    # test 'creates a tls server to read and write data'
    # test 'creates a tls server to read and write data using IPv6'

    # many tests about certops

    # test 'does not resolve name of client address in default'
    # test 'does resolve name of client address if resolve_name is true'
    # test 'can keep connections alive for tls if keepalive specified' do
    #   pend "not implemented yet"
    # end

    # test 'raises error if plugin registers data callback for connection object from #server_create'
    # test 'can call write_complete callback if registered'
    # test 'can call before_close callback to send data if registered'
    # test 'can call close callback if registered'
  end

  sub_test_case '#server_create_unix' do
    # not implemented yet

    # test 'can accept all keyword arguments valid for unix server'
    # test 'creates a unix server just to read data'
    # test 'creates a unix server to read and write data'

    # test 'raises error if plugin registers data callback for connection object from #server_create'
    # test 'can call write_complete callback if registered'
    # test 'can call before_close callback to send data if registered'
    # test 'can call close callback if registered'
  end

  # run tests for tcp, tls and unix
  sub_test_case '#server_create_connection' do
    # def server_create_connection(title, port, proto: :tcp, bind: '0.0.0.0', shared: true, certopts: nil, resolve_name: false, linger_timeout: 0, backlog: nil, &block)
    test 'raise error if block argument is not specified or too many'

    test 'raise error if udp is specified in proto' do
      assert_raise(ArgumentError.new("BUG: cannot create connection for UDP")) do
        @d.server_create_connection(:myserver, PORT, proto: :udp){|c| c }
      end
    end

    test 'does not resolve name of client address in default'
    test 'does resolve name of client address if resolve_name is true'

    test 'creates a server to provide connection, which can read, write and close'
    test 'creates a server to provide connection, which accepts callbacks for data, write_complete, before_close and close'
    test 'creates a server to provide connection, which serializes close to call it exactly once'

    test 'can keep connections alive for tcp/tls if keepalive specified' do
      pend "not implemented yet"
    end
  end

end
