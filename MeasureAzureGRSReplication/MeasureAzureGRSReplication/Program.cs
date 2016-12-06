using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace MeasureAzureGRSReplication
{
	class Program
	{
		const int BUFFER_SIZE = 1024;
		const string CONTAINER_NAME = "uploads";
		const int MEASURE_COUNT = 10;
		static void Main(string[] args)
		{
			// Parse the connection string and return a reference to the storage account.
			CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
				CloudConfigurationManager.GetSetting("StorageConnectionString"));

			CloudBlobClient primaryBlobClient = storageAccount.CreateCloudBlobClient();


			CloudStorageAccount secondaryStorageAccount = CloudStorageAccount.Parse(
				CloudConfigurationManager.GetSetting("StorageConnectionString"));

			CloudBlobClient secondaryBlobClient = secondaryStorageAccount.CreateCloudBlobClient();
			secondaryBlobClient.DefaultRequestOptions.LocationMode = LocationMode.SecondaryOnly;

			// get replication stats
			var stats = secondaryBlobClient.GetServiceStats();
			Console.WriteLine("Geo replication status: {0}, last sync: {1}", stats.GeoReplication.Status, stats.GeoReplication.LastSyncTime);

			// Retrieve a reference to a container.
			CloudBlobContainer container = primaryBlobClient.GetContainerReference(CONTAINER_NAME);

			// Create the container if it doesn't already exist.
			container.CreateIfNotExists();

			// generate random data
			Random rand = new Random();
			byte[] buffer = new byte[BUFFER_SIZE];
			rand.NextBytes(buffer);

			var measureResults = new long[MEASURE_COUNT];
			for (int i = 0; i < MEASURE_COUNT; i++)
			{
				// upload data to blob
				Console.WriteLine("Uploading blob #{0} to primary.", i);
				var blobName = Guid.NewGuid().ToString();
				CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);
				blockBlob.UploadFromByteArray(buffer, 0, BUFFER_SIZE);
				Console.WriteLine("Blob #{0} upload to primary complete.", i);

				Stopwatch watch = Stopwatch.StartNew();
				CloudBlockBlob secondaryBlockBlob = null;
				do
				{
					var secondaryContainer = secondaryBlobClient.GetContainerReference(CONTAINER_NAME);
					if (secondaryContainer != null)
					{
						secondaryBlockBlob = secondaryContainer.GetBlockBlobReference(blobName);
						if (secondaryBlockBlob != null)
						{
							byte[] downloadBuffer = new byte[BUFFER_SIZE];
							secondaryBlockBlob.DownloadToByteArray(downloadBuffer, 0);
							measureResults[i] = watch.ElapsedMilliseconds;
							Console.WriteLine("#{1} Replication lag {0}ms", measureResults[i], i);
							break;
						}
					}
				} while (true);
			}
			// calculate average time
			var average = Convert.ToInt64(measureResults.Average());
			Console.WriteLine("Average replication time {0}ms", average);
			Console.ReadLine();
		}
	}
}
