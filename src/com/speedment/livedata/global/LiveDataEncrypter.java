package com.speedment.livedata.global;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class LiveDataEncrypter {

	public final static String	ENCRYPTION_PASSWORD = "speedment";
	public final static String	ENCRYPTION_PROPERTIES_PREFIX = "encrypted:";
	
	public String	generatePassword (String sPassword)
	{
		String sEncrpytedPassword = sPassword;
		try {
			SecretKeySpec	key = generatePasswordKey (ENCRYPTION_PASSWORD);
			sEncrpytedPassword = encrypt (sPassword, key);
		} catch (Exception e) {}
		return sEncrpytedPassword;
	}
	
	public String encrypt (String sStringToEncrypt)
	{
		String sEncrpytedString = sStringToEncrypt;
		try {
			SecretKeySpec	key = generatePasswordKey (ENCRYPTION_PASSWORD);
			sEncrpytedString = encrypt (sStringToEncrypt, key);
		} catch (Exception e) {}
		return sEncrpytedString;
	}
	
	public String decryptIfEncrypted (String sStringToDecrypt)
	{
		String sDecrpytedString = sStringToDecrypt;
		if (sStringToDecrypt.startsWith(ENCRYPTION_PROPERTIES_PREFIX))
			sDecrpytedString = decrypt(sStringToDecrypt.substring(ENCRYPTION_PROPERTIES_PREFIX.length()));
		return sDecrpytedString;
	}

	public String decrypt(String sStringToDecrypt)
	{
		String sDecrpytedString = sStringToDecrypt;
		
		try {
			SecretKeySpec	key = generatePasswordKey (ENCRYPTION_PASSWORD);
			sDecrpytedString = decrypt (sStringToDecrypt, key);
		} catch (Exception e) {}
		return sDecrpytedString;
	}

    public SecretKeySpec generatePasswordKey(String sEncryptionPassword) throws NoSuchAlgorithmException, InvalidKeySpecException 
    {
    	SecretKeySpec key = null;
	     if (sEncryptionPassword != null) 
	     {
	         // The salt (probably) can be stored along with the encrypted data
	         byte[] salt = new String("SPEEDMENT").getBytes();

	         // Decreasing this speeds down startup time and can be useful during testing, but it also makes it easier for brute force attackers
	         int iterationCount = 40000;
	         // Other values give me java.security.InvalidKeyException: Illegal key size or default parameters
	         int keyLength = 128;
	         key = createSecretKey(sEncryptionPassword.toCharArray(),
	                 salt, iterationCount, keyLength);
	     }
	     return key;
    }
    
    private SecretKeySpec createSecretKey(char[] password, byte[] salt, int iterationCount, int keyLength) throws NoSuchAlgorithmException, InvalidKeySpecException {
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        PBEKeySpec keySpec = new PBEKeySpec(password, salt, iterationCount, keyLength);
        SecretKey keyTmp = keyFactory.generateSecret(keySpec);
        return new SecretKeySpec(keyTmp.getEncoded(), "AES");
    }

    private String encrypt(String property, SecretKeySpec key) throws GeneralSecurityException, UnsupportedEncodingException {
        Cipher pbeCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        pbeCipher.init(Cipher.ENCRYPT_MODE, key);
        AlgorithmParameters parameters = pbeCipher.getParameters();
        IvParameterSpec ivParameterSpec = parameters.getParameterSpec(IvParameterSpec.class);
        byte[] cryptoText = pbeCipher.doFinal(property.getBytes("UTF-8"));
        byte[] iv = ivParameterSpec.getIV();
        return base64Encode(iv) + ":" + base64Encode(cryptoText);
    }

    private String decrypt(String string, SecretKeySpec key) throws GeneralSecurityException, IOException {
        String iv = string.split(":")[0];
        String property = string.split(":")[1];
        Cipher pbeCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        pbeCipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(base64Decode(iv)));
        return new String(pbeCipher.doFinal(base64Decode(property)), "UTF-8");
    }

    private byte[] base64Decode(String property) throws IOException {
        return Base64.getDecoder().decode(property);
    }

    private String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

}
