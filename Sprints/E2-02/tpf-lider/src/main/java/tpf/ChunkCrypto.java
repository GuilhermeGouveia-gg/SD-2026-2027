package tpf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.HexFormat;
import javax.crypto.*;
import javax.crypto.spec.*;

/** Cifra autenticada feita exclusivamente com as APIs do JDK. */
public final class ChunkCrypto {
    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RNG = new SecureRandom();
    private final byte[] masterKey;

    // Mantém o nome e a estrutura indicados no material de partida do docente.
    public static final class Sealed {
        public final String id;
        public final int keyVersion;
        public final byte[] salt, nonce, ciphertext;

        public Sealed(String id, int keyVersion, byte[] salt, byte[] nonce, byte[] ciphertext) {
            this.id = id;
            this.keyVersion = keyVersion;
            this.salt = salt.clone();
            this.nonce = nonce.clone();
            this.ciphertext = ciphertext.clone();
        }
    }

    public ChunkCrypto(byte[] masterKey) {
        if (masterKey.length != 32) throw new IllegalArgumentException("A master key deve ter 32 bytes");
        this.masterKey = masterKey.clone();
    }

    static byte[] hkdf(byte[] ikm, byte[] salt, byte[] info) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        // Extract: o salt é a chave do primeiro HMAC.
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(ikm);
        // Expand: 32 bytes cabem num bloco SHA-256, com contador 0x01.
        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        mac.update(info);
        mac.update((byte) 0x01);
        return mac.doFinal();
    }

    static byte[] aad(int version, byte[] salt, byte[] nonce) {
        return ByteBuffer.allocate(6 + 4 + salt.length + nonce.length)
                .put("tpf-v1".getBytes(StandardCharsets.US_ASCII))
                .putInt(version).put(salt).put(nonce).array();
    }

    public static String sha256Hex(byte[] dados) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
    }

    public Sealed seal(byte[] plaintext) throws GeneralSecurityException {
        byte[] salt = new byte[32], nonce = new byte[12];
        RNG.nextBytes(salt);
        RNG.nextBytes(nonce);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(hkdf(masterKey, salt, INFO), "AES"),
                new GCMParameterSpec(128, nonce));
        cipher.updateAAD(aad(1, salt, nonce));
        byte[] ciphertext = cipher.doFinal(plaintext); // Inclui a tag GCM de 16 bytes.
        return new Sealed(sha256Hex(ciphertext), 1, salt, nonce, ciphertext);
    }

    public byte[] open(Sealed s) throws GeneralSecurityException {
        // A ordem é intencional: endereço por conteúdo, versão e autenticação.
        if (!sha256Hex(s.ciphertext).equals(s.id)) throw new GeneralSecurityException("Hash incorreto");
        if (s.keyVersion != 1) throw new GeneralSecurityException("Versão de chave desconhecida");
        if (s.salt.length != 32 || s.nonce.length != 12) throw new GeneralSecurityException("Cabeçalho inválido");
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(hkdf(masterKey, s.salt, INFO), "AES"),
                new GCMParameterSpec(128, s.nonce));
        cipher.updateAAD(aad(s.keyVersion, s.salt, s.nonce));
        return cipher.doFinal(s.ciphertext);
    }
}
