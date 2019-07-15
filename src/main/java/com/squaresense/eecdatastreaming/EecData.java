package com.squaresense.eecdatastreaming;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.beans.Transient;
import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EecData {

    private String meter ;
    private Instant beginDate;
    private Instant endDate;
    private BigDecimal energyConsumption = BigDecimal.ZERO;
    private BigDecimal meanPower = BigDecimal.ZERO;
    private BigDecimal minPower = BigDecimal.ZERO;
    private BigDecimal maxPower = BigDecimal.ZERO;
    private transient int cp = 0;
    private transient BigDecimal predIndex = BigDecimal.ZERO;

    public EecData aggregate(EecDataEvent event){
        this.minPower = event.getPower().min(minPower);
        this.maxPower = event.getPower().max(maxPower);
        this.meanPower = this.meanPower.add(event.getPower());
        this.energyConsumption = energyConsumption.add(event.getEnergy().add(predIndex.negate()));
        this.predIndex = event.getEnergy();
        this.cp ++;
        return this;
    }
}
