public class QueryEquivalenceJobResult extends JobResult {

    private boolean isProven = false;
    private boolean isCompiled = false;
    private long verificationTime = 0;

    private String message;
    private String errorMessage;


    public void setMessage(String message) {
        this.message = message;
    }

    public void setProven(boolean proven) {
        isProven = proven;
    }

    public boolean isCompiled() {
        return isCompiled;
    }

    public void setCompiled(boolean compiled) {
        isCompiled = compiled;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setVerificationTime(long verificationTime) {
        this.verificationTime = verificationTime;
    }

}
