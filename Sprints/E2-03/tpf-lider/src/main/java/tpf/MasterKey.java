package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.security.SecureRandom;
import java.util.*;

/** Cria uma chave local, sem nunca a mostrar em mensagens ou logs. */
public final class MasterKey {
    private MasterKey() { }

    public static byte[] carregar(Path caminho) throws IOException {
        if (Files.notExists(caminho)) {
            // CREATE_NEW impede substituir acidentalmente uma chave já existente.
            try {
                if (Files.getFileStore(caminho.toAbsolutePath().getParent()).supportsFileAttributeView("posix"))
                    Files.createFile(caminho, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
                else Files.createFile(caminho);
                proteger(caminho, true);
                byte[] chave = new byte[32];
                new SecureRandom().nextBytes(chave);
                try (FileChannel out = FileChannel.open(caminho, StandardOpenOption.WRITE)) {
                    ByteBuffer buffer = ByteBuffer.wrap(chave);
                    while (buffer.hasRemaining()) out.write(buffer);
                    out.force(true);
                }
                proteger(caminho, false);
            } catch (FileAlreadyExistsException e) {
                // Outro arranque criou a chave; validá-la abaixo, sem a substituir.
            }
        }
        byte[] chave = Files.readAllBytes(caminho);
        if (chave.length != 32) throw new IOException("master.key deve conter exatamente 32 bytes");
        proteger(caminho, false);
        return chave;
    }

    private static void proteger(Path caminho, boolean escrita) throws IOException {
        PosixFileAttributeView posix = Files.getFileAttributeView(caminho, PosixFileAttributeView.class);
        if (posix != null) {
            posix.setPermissions(PosixFilePermissions.fromString(escrita ? "rw-------" : "r--------"));
            return;
        }
        // No Windows, a API ACL do Java não expõe a proteção contra herança.
        // Construir uma DACL protegida com a ferramenta nativa, antes de escrever a chave.
        AclFileAttributeView acl = Files.getFileAttributeView(caminho, AclFileAttributeView.class);
        if (acl == null) throw new IOException("Sistema de ficheiros sem permissões POSIX/ACL");
        String script = "$ErrorActionPreference='Stop'; "
                + "$p=$env:TPF_KEY_PATH; $old=[System.IO.File]::GetAccessControl($p); "
                + "$owner=$old.GetOwner([System.Security.Principal.SecurityIdentifier]); "
                // Alterar só a DACL. SetOwner também pediria WRITE_OWNER na gravação:
                // essa permissão já não existe depois da primeira restrição da chave.
                + "$a=$old; $a.SetAccessRuleProtection($true,$false); "
                + "$rules=@($a.GetAccessRules($true,$false,[System.Security.Principal.SecurityIdentifier])); "
                + "foreach($existing in $rules){$a.RemoveAccessRuleSpecific($existing)}; "
                + "$rule=[System.Security.AccessControl.FileSystemAccessRule]::new($owner,'"
                + (escrita ? "Read, Write" : "Read") + "','Allow'); "
                + "$a.AddAccessRule($rule); [System.IO.File]::SetAccessControl($p,$a)";
        ProcessBuilder builder = new ProcessBuilder("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script);
        // O caminho vai numa variável de ambiente: nunca é interpolado em código PowerShell.
        builder.environment().put("TPF_KEY_PATH", caminho.toAbsolutePath().toString());
        builder.redirectErrorStream(true);
        Process processo = builder.start();
        try {
            String resultado = new String(processo.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            if (processo.waitFor() != 0) throw new IOException("Não foi possível proteger master.key: " + resultado);
        } catch (InterruptedException e) {
            processo.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IOException("Proteção da chave interrompida", e);
        }
        // Falhar de forma explícita se o sistema não respeitou a DACL pedida.
        List<AclEntry> entradas = acl.getAcl();
        if (entradas.size() != 1 || !entradas.get(0).principal().equals(acl.getOwner())
                || entradas.get(0).permissions().contains(AclEntryPermission.WRITE_DATA) != escrita)
            throw new IOException("As permissões de master.key não ficaram restritas ao dono");
    }
}
