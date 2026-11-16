package raft04;

public final class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        if (term < 1 || voter < 1 || candidate < 1) {
            throw new IllegalArgumentException("VOTO inválido");
        }
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return Wire.encodeVoteReply(term, voter, candidate, granted);
    }

    public static VoteReply parse(String line) {
        String[] f = line.trim().split(";", -1);
        if (f.length != 5 || !f[0].equals("VOTO")) {
            throw new IllegalArgumentException("VOTO mal formado: " + line);
        }
        return new VoteReply(Long.parseLong(f[1]), Integer.parseInt(f[2]), Integer.parseInt(f[3]), "S".equals(f[4]));
    }

    public static VoteReply decode(String line) {
        return parse(line);
    }
}
