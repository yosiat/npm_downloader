require 'net/http'
require 'json'
require 'time'
require 'json'
require 'fileutils'
require 'open-uri'
require 'parallel'


PACKAGES_DIRECTORY = "/Users/yosi/code/npm_downloader/packages"


class SkimRegistry


	def initialize registry_url
		@registry_url = registry_url
	end


	def changes_since registry_seq
		changes_url = URI(@registry_url + "/_changes?since=" + registry_seq.to_s)
		changes = JSON.parse Net::HTTP.get(changes_url) 
		changes["results"]
	end

	def package_by_id id
		package_url = URI("#{@registry_url}/#{id}?att_encoding_info=true")
		JSON.parse Net::HTTP.get(package_url)
	end

end


class Package
	attr_reader :id

	def initialize package_id, repository
		@id = package_id
		@package_metadata = repository.package_by_id package_id 
	end


	def versions_since time
		versions = @package_metadata["time"].select { |version, vtime| vtime.is_a?(String) and Time.parse(vtime) > time }
		versions.keys.reject { |version| version == "modified" or version == "created" }
	end


	def download_changes_since time
		write_package_metadata

		if is_valid_package
			versions_to_download = versions_since time

			if not versions_to_download.empty?
				puts "Need to download #{versions_to_download.length} versions for #{@id}"
				download_attachments versions_to_download
			end
		end
	end

	def is_valid_package
		valid_key? "versions" and valid_key? "time"
	end

	def valid_key? key
		@package_metadata.has_key?(key)  and not @package_metadata[key].empty?
	end



	def download_attachments versions_to_download
		versions = @package_metadata["versions"].values_at(*versions_to_download)

		write_hashes versions

		FileUtils.mkdir_p "#{directory}/versions"

		versions.each { |version| download_attachment(version) }
	end

	def download_attachment attachment
		tarball_url = attachment["dist"]["tarball"]
		puts "Downloading tarball #{tarball_url} for #{@id}"

		tarball_dir = "#{directory}/versions/#{@id}-#{attachment["version"]}.tgz"
		IO.copy_stream open(tarball_url), tarball_dir 
	end


	def directory
		"#{PACKAGES_DIRECTORY}/#{@id}"
	end

	def write_hashes versions
		hashes = versions.map { |version| "#{version["version"]},#{version["dist"]["shasum"]}" }
		File.open "#{directory}/versions_hash.csv", "w+" do |file|
			file.write hashes.join "\n"
			file.flush
		end
	end


	def write_package_metadata
		puts "Writing package metadata to #{directory}"	
		FileUtils.mkdir_p directory
		File.open "#{directory}/package.json", "w+" do |file|
			file.write @package_metadata.to_json
			file.flush
		end
	end

end


def write_change_error(change, e)
	File.open "./errors", "a+" do |file|
		file.write "#{change["id"]},#{e.message}\n"
		file.flush
	end
end

def write_change_success change
	File.open "./success", "a+" do |file|
	  file.write "#{change["id"]}\n"
		file.flush
	end
end
	




registry = SkimRegistry.new("https://skimdb.npmjs.com/registry")
fetch_time = Time.new(2015, 4, 4)
changes = registry.changes_since 1063507

puts "Downloading #{changes.count} changes"
Parallel.each(changes, :in_processes => 48) do |change|
	begin
		package = Package.new change["id"], registry
		package.download_changes_since fetch_time
	rescue Exception => e
		puts "Failed to download #{change["id"]} - #{e.message}"
		write_change_error(change, e)
	else
		write_change_success change
	end
end

