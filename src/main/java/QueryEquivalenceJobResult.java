import java.util.UUID;

public class QueryEquivalenceJobResult extends JobResult {

    private boolean isProven = false;
    private boolean isCompiled = false;
    private long verificationTime = 0;

    private String message;
    private String errorMessage;


    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isProven() {
        return isProven;
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

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getVerificationTime() {
        return verificationTime;
    }

    public void setVerificationTime(long verificationTime) {
        this.verificationTime = verificationTime;
    }

}
