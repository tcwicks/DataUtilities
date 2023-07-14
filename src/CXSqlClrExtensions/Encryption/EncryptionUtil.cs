using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.Encryption
{
    public static class EncryptionUtil
    {
        private static byte[] RandomSalt()
        {
            return RandomBytes(16);
        }

        private static byte[] RandomBytes(int _length)
        {
            using (RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider())
            {
                byte[] nonce = new byte[_length];
                rng.GetBytes(nonce);
                return nonce;
            }
        }

        public static void GenerateKeys(string password, out byte[] salt, out byte[] key)
        {
            salt = RandomSalt();
            GenerateKeys(password, salt, out key);
        }
        public static void GenerateKeys(string password, byte[] salt, out byte[] key)
        {
            int iterations = 11000;
            int keyLength = 32; // 32 bytes equal 256 bits.
            HashAlgorithmName hashMethod = HashAlgorithmName.SHA256;
            using (Rfc2898DeriveBytes keyGenerator = new Rfc2898DeriveBytes(password, salt, iterations, hashMethod))
            {
                key = keyGenerator.GetBytes(keyLength);
            }
        }

        public static byte[] Encrypt(string settingsKey, string content, out byte[] salt)
        {
            salt = RandomSalt();
            return Encrypt(settingsKey, content, salt);
        }
        private const string RandomGuid = @"af024812-260b-4384-9b40-aef660476f46";
        public static byte[] Encrypt(string settingsKey, string content, byte[] salt)
        {
            byte[] key;
            byte[] ContentBytes;
            GenerateKeys(string.Concat(settingsKey, RandomGuid), salt, out key);
            //ContentBytes = System.Text.ASCIIEncoding.ASCII.GetBytes(content);

            ContentBytes = EncryptStringToBytes_Aes(content, key, salt);
            ContentBytes = ProtectedData.Protect(ContentBytes, key, DataProtectionScope.CurrentUser);
            ContentBytes = ProtectedData.Protect(ContentBytes, key, DataProtectionScope.LocalMachine);
            return ContentBytes;
        }
        public static string Decrypt(string settingsKey, byte[] contentBytes, byte[] salt)
        {
            byte[] key;
            byte[] DecryptedBytes;
            GenerateKeys(string.Concat(settingsKey, RandomGuid), salt, out key);
            DecryptedBytes = ProtectedData.Unprotect(contentBytes, key, DataProtectionScope.LocalMachine);
            DecryptedBytes = ProtectedData.Unprotect(DecryptedBytes, key, DataProtectionScope.CurrentUser);
            //return ASCIIEncoding.ASCII.GetString(DecryptedBytes);
            return DecryptStringFromBytes_Aes(DecryptedBytes, key, salt);
        }

        static byte[] EncryptStringToBytes_Aes(string plainText, byte[] Key, byte[] IV)
        {
            // Check arguments.
            if (plainText == null || plainText.Length <= 0)
                throw new ArgumentNullException("plainText");
            if (Key == null || Key.Length <= 0)
                throw new ArgumentNullException("Key");
            if (IV == null || IV.Length <= 0)
                throw new ArgumentNullException("IV");
            byte[] encrypted;

            // Create an Aes object
            // with the specified key and IV.
            using (Aes aesAlg = Aes.Create())
            {
                aesAlg.Mode = CipherMode.CBC;
                aesAlg.Padding = PaddingMode.PKCS7;
                aesAlg.KeySize = 256;

                aesAlg.Key = Key;
                aesAlg.IV = IV;


                // Create an encryptor to perform the stream transform.
                ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);

                // Create the streams used for encryption.
                using (MemoryStream msEncrypt = new MemoryStream())
                {
                    using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter swEncrypt = new StreamWriter(csEncrypt))
                        {
                            //Write all data to the stream.
                            swEncrypt.Write(plainText);
                        }
                        encrypted = msEncrypt.ToArray();
                    }
                }
            }

            // Return the encrypted bytes from the memory stream.
            return encrypted;
        }

        static string DecryptStringFromBytes_Aes(byte[] cipherText, byte[] Key, byte[] IV)
        {
            // Check arguments.
            if (cipherText == null || cipherText.Length <= 0)
                throw new ArgumentNullException("cipherText");
            if (Key == null || Key.Length <= 0)
                throw new ArgumentNullException("Key");
            if (IV == null || IV.Length <= 0)
                throw new ArgumentNullException("IV");

            // Declare the string used to hold
            // the decrypted text.
            string plaintext = null;

            // Create an Aes object
            // with the specified key and IV.
            using (Aes aesAlg = Aes.Create())
            {
                aesAlg.Mode = CipherMode.CBC;
                aesAlg.Padding = PaddingMode.PKCS7;
                aesAlg.KeySize = 256;

                aesAlg.Key = Key;
                aesAlg.IV = IV;

                // Create a decryptor to perform the stream transform.
                ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);

                // Create the streams used for decryption.
                using (MemoryStream msDecrypt = new MemoryStream(cipherText))
                {
                    using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                    {
                        using (StreamReader srDecrypt = new StreamReader(csDecrypt))
                        {

                            // Read the decrypted bytes from the decrypting stream
                            // and place them in a string.
                            plaintext = srDecrypt.ReadToEnd();
                        }
                    }
                }
            }

            return plaintext;
        }
    }

}
