package utils;

import org.apache.spark.Partitioner;

import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CustomDataPartitioner extends Partitioner {
    private int numPartitions;
    private final LocalDate initialDate;

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public CustomDataPartitioner(String initialDate) {
        super();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        this.initialDate = LocalDate.parse(initialDate, dtf);
    }

    @Override
    public int getPartition(Object arg0) {
        Covid1Data covid = (Covid1Data) arg0;

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate currentDate = LocalDate.parse(covid.getData(), dtf);

        long daysBetween = Duration.between(initialDate, currentDate).toDays();

        return (int) (daysBetween % 7);
    }

    @Override
    public int numPartitions() {
        // TODO Auto-generated method stub
        return getNumPartitions();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustomDataPartitioner) {
            CustomDataPartitioner partitionerObject = (CustomDataPartitioner) obj;
            if (partitionerObject.getNumPartitions() == this.getNumPartitions())
                return true;
        }
        return false;
    }
}
