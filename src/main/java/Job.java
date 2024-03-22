import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;


public abstract class Job<T extends JobResult> implements Callable<Job<T>> {

    protected UUID id;
    protected T result;
    protected String name;

    public Job(String name) {
        this(UUID.randomUUID(), name);

    }

    public Job(UUID id, String name) {
        this.name = name;
        this.id = id;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public void randomId() {
        this.id = UUID.randomUUID();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return Objects.equals(name, job.name) && id.equals(job.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
