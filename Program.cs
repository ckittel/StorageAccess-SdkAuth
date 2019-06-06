using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace StorageAccess
{
    class Program
    {
        enum AuthMode
        {
            ConnectionString,
            SasToken,
            AppRegistration,
            ManagedIdentity
        }
        private const string AccountName = "ACCTNAME";
        private const string AccountKey = "DONTDOTHIS";
        private const string ReadWriteSasKey = "SASKEY";
        private const string ClientId = "MIP_CLIENTID";
        private const string ClientSecret = "MIP_CLIENTSECRET";
        private const string Authority = "https://login.microsoftonline.com/foo.com";

        static async Task Main(string[] args)
        {
            var authMode = AuthMode.ConnectionString;

            // Create a CloudBlobClient using various auth methods
            CloudBlobClient cloudBlobClient = null;

            switch (authMode)
            {
                case AuthMode.ConnectionString:
                    // Account Key
                    // Local Emulator
                    // Technically can do SAS here as well

                    // Local Emulator (direct)
                    //cloudBlobClient = CloudStorageAccount.DevelopmentStorageAccount
                    //                                     .CreateCloudBlobClient();

                    //cloudBlobClient = CloudStorageAccount.Parse("UseDevelopmentStorage=true")
                    //                                     .CreateCloudBlobClient();

                    // Account Key-Based Connection String
                    cloudBlobClient = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={AccountName};AccountKey={AccountKey};EndpointSuffix=core.windows.net")
                                                         .CreateCloudBlobClient();

                    break;

                case AuthMode.SasToken:
                    var sasCredentials = new StorageCredentials(ReadWriteSasKey);
                    
                    cloudBlobClient = new CloudStorageAccount(sasCredentials, AccountName, endpointSuffix: null, useHttps: true).CreateCloudBlobClient();

                    break;

                case AuthMode.AppRegistration:
                case AuthMode.ManagedIdentity:
                    // Get an access token
                    string accessToken = null;

                    if (AuthMode.ManagedIdentity == authMode)
                    {
                        // MSI when on Azure
                        // Visual Studio (Azure Service Authentication)
                        // Command Line (az login)
                        var msiTokenProvider = new AzureServiceTokenProvider();
                        accessToken = await msiTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

                        // No emulator support
                    }
                    else
                    {
                        // Anywhere, with the app's own ID
                        var clientApp = ConfidentialClientApplicationBuilder.Create(ClientId)
                            .WithClientSecret(ClientSecret)
                            .WithAuthority(Authority)
                            .Build();

                        var authResult = await clientApp.AcquireTokenForClient(new[] { "https://storage.azure.com/.default" }).ExecuteAsync();
                        accessToken = authResult.AccessToken;

                        // No emulator support
                    }

                    var tokenCredentials = new StorageCredentials(new TokenCredential(accessToken));

                    cloudBlobClient = new CloudStorageAccount(tokenCredentials, AccountName, endpointSuffix: null, useHttps: true).CreateCloudBlobClient();

                    break;

                default:
                    throw new InvalidOperationException();
            }


            // Grab reference to the container we'll use for all the samples
            var container = cloudBlobClient.GetContainerReference("my-files");

      
            // List all files in the container
            Console.WriteLine("LISTING FILES in example-container...");

            var blobNames = new List<string>();

            // Loop through the blob metadata grabbing the name
            BlobContinuationToken bct = null;
            do
            {
                var result = await container.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, null, bct, null, null);

                blobNames.AddRange(result.Results.OfType<CloudBlob>().Select(b => b.Name));

                bct = result.ContinuationToken;
            } while (bct != null);

            // Sort them for my OCD purposes
            blobNames.Sort();

            foreach (var filename in blobNames)
            {
                Console.WriteLine(filename);
            }

            Console.WriteLine("...done" + Environment.NewLine);



            // Add a new text file
            Console.WriteLine("Uploading text file my-text.txt...");

            // Grab a reference to the blob and upload (overwritting if exists)
            var newBlob = container.GetBlockBlobReference("my-text.txt");
            await newBlob.UploadTextAsync("I really love blobs!");

            Console.WriteLine("...done" + Environment.NewLine);



            // Read the file
            Console.WriteLine("Reading text file my-text.txt...");

            // Grab a reference to the blob and download
            var existingBlob = container.GetBlockBlobReference("my-text.txt");
            Console.WriteLine(await existingBlob.DownloadTextAsync());

            Console.WriteLine("...done" + Environment.NewLine);



            // Move that file to a diffent name
            Console.WriteLine("Moving my-text.txt to your-text.txt...");

            // Get a handle on both files in the container
            var sourceBlob = container.GetBlockBlobReference("my-text.txt");
            newBlob = container.GetBlockBlobReference("your-text.txt");

            // Move the blob
            if (await sourceBlob.ExistsAsync())
            {
                // This does a server-side async copy
                await newBlob.StartCopyAsync(sourceBlob);

                // Wait for the copy to complete
                while (CopyStatus.Pending == newBlob.CopyState.Status)
                {
                    await Task.Delay(100);
                    await newBlob.FetchAttributesAsync();
                }

                // 86 the original file
                await sourceBlob.DeleteIfExistsAsync();
            }

            Console.WriteLine("...done" + Environment.NewLine);



            // Read the moved file
            Console.WriteLine("Reading text file your-text.txt...");

            // Grab a reference to the blob and download
            existingBlob = container.GetBlockBlobReference("your-text.txt");
            Console.WriteLine(await existingBlob.DownloadTextAsync());

            Console.WriteLine("...done");
        }
    }
}
