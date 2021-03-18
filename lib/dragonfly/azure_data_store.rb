require 'dragonfly'
require 'azure/storage/file'
require 'yaml'

Dragonfly::App.register_datastore(:azure) { Dragonfly::AzureDataStore }

module Dragonfly
  class AzureDataStore
    attr_accessor :account_name, :access_key, :container_name, :root_path,
                  :url_scheme, :url_host, :store_meta, :legacy_meta

    def initialize(opts = {})
      @account_name = opts[:account_name]
      @access_key = opts[:access_key]
      @container_name = opts[:container_name]
      @root_path = opts[:root_path]
      @url_scheme = opts[:url_scheme] || 'http'
      @url_host = opts[:url_host]
      @store_meta = opts[:store_meta].nil? ? true : opts[:store_meta]
      @legacy_meta = opts[:legacy_meta]
    end

    def write(content, _opts = {})
      uid = path_for(content.name || 'file')
      directory_path = full_path(uid).split("/")[0...-1].join("/")
      filename = full_path(uid).split("/").last
      create_parent_directory "", directory_path
      options = {}
      options[:metadata] = content.meta if store_meta
      content.file do |f|
        storage(:create_file_from_content, container_name, directory_path, filename, content.size, f, options)
      end
      uid
    end

    def read(uid)
      begin
        tries ||= 2
        path = full_path(uid)
        result, body = storage(:get_file, container_name, root_path, uid)
        meta = result.metadata
        meta = meta_from_file(uid) if legacy_meta && (meta.nil? || meta.empty?)
        if meta.nil? || meta.empty?
          directory_path = full_path(uid).split("/")[0...-1].join("/")
          filename = full_path(uid).split("/").last
          storage(:set_file_metadata, container_name, directory_path, filename, {name: filename}, options = {})
          read(uid)
        else
          [body, meta]
        end      
      rescue Azure::Core::Http::HTTPError
        raise if (tries -= 1).zero?
        retry
      end
    end

    # Updates metadata of file and deletes old meta file from legacy mode.
    #
    def update_metadata(uid)
      return false unless store_meta
      path = full_path(uid)
      meta = storage(:get_file, container_name, root_path, uid)[0].metadata
      return false if meta.present?
      meta = meta_from_file(uid)
      return false if meta.blank?
      filename = full_path(uid).split("/").last.gsub('.meta', '')
      storage(:set_file_metadata,container_name, root_path, filename, metadata, options = {})
      storage(:delete_file, container_name, root_path, uid)
      true
    #rescue Azure::Core::Http::HTTPError
      #nil
    end

    def destroy(uid)
      filename = full_path(uid).split("/").last
      directory_path = full_path(uid).split("/")[0...-1].join("/")
      storage(:delete_file, container_name, directory_path, filename, options = {})
      true
    rescue Azure::Core::Http::HTTPError
      false
    end

    def url_for(uid, opts = {})
      scheme = opts[:scheme] || url_scheme
      host   = opts[:host]   || url_host ||
               "#{account_name}.file.core.windows.net"
      "#{scheme}://#{host}/#{container_name}/#{full_path(uid)}"
    end

    private

    def storage(method, *params)
      tries ||= 2
      @storage ||=
        Azure::Storage::File::FileService.create(
          storage_account_name: account_name,
          storage_access_key: access_key
        )
      @storage.send(method, *params)
    rescue Faraday::ConnectionFailed
      raise if (tries -= 1).zero?
      retry
    end

    def container
      @container ||= begin
        storage(:get_container_properties, container_name)
      rescue Azure::Core::Http::HTTPError => e
        raise if e.status_code != 404
        storage(:create_container, container_name)
      end
    end

    def path_for(filename)
      time = Time.now
      "#{time.strftime '%Y/%m/%d/'}#{rand(1e15).to_s(36)}_#{filename.gsub(/[^\w.]+/, '_')}"
    end

    def full_path(filename)
      File.join(*[root_path, filename].compact)
    end

    def meta_path(path)
      "#{path}.meta.yml"
    end

    def meta_from_file(uid)
      filename = full_path(uid).split("/").last
      directory_path = full_path(uid).split("/")[0...-1].join("/")
      meta_file = storage(:get_file, container_name, directory_path, meta_path(filename), {})
      meta_file[1]
      YAML.safe_load(meta_file[1])
    rescue Azure::Core::Http::HTTPError
      {}
    end

    def create_parent_directory(path, directory_path)
      directory_path_array = (directory_path.split("/")- [""])
      first_parent = directory_path_array.first
      first_parent_path = "#{path}/#{first_parent}"
      return unless first_parent.present?
      storage(:create_directory, container_name, first_parent_path)
      create_parent_directory(first_parent_path, directory_path_array.drop(1).try(:join, '/'))
    rescue Azure::Core::Http::HTTPError
      create_parent_directory(first_parent_path, directory_path_array.drop(1).try(:join, '/'))
    end
  end
end
