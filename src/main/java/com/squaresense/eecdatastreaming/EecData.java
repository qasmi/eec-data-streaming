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
public class EecData {

    private BigDecimal energyConsumption;
    private BigDecimal meanPower;
    private BigDecimal minPower;
    private BigDecimal maxPower;
    private transient int count;
    private transient BigDecimal predIndex;

    public EecData(){
        this.energyConsumption =  BigDecimal.ZERO;
        this.meanPower =  BigDecimal.ZERO;
        this.minPower =  BigDecimal.ZERO;
        this.maxPower =  BigDecimal.ZERO;
        this.count = 0;
        this.predIndex =  BigDecimal.ZERO;
    }

    public EecData(BigDecimal energyConsumption, BigDecimal meanPower, BigDecimal minPower, BigDecimal maxPower, int count, BigDecimal predIndex) {
        this.energyConsumption = energyConsumption;
        this.meanPower = meanPower;
        this.minPower = minPower;
        this.maxPower = maxPower;
        this.count = count;
        this.predIndex = predIndex;
    }

    public EecData(EecDataEvent leftReducer, EecDataEvent rigthReducer) {
        this.minPower = leftReducer.getPower().min(rigthReducer.getPower());
        this.maxPower = leftReducer.getPower().max(rigthReducer.getPower());
        this.meanPower = leftReducer.getPower().add(rigthReducer.getPower());
        this.energyConsumption = leftReducer.getEnergy().add(rigthReducer.getEnergy().add(predIndex.negate()));
        this.predIndex = rigthReducer.getEnergy();
    }

    public EecData aggregate(EecDataEvent event){
        this.minPower = event.getPower().min(minPower);
        this.maxPower = event.getPower().max(maxPower);
        this.meanPower = this.meanPower.add(event.getPower());
        this.energyConsumption = energyConsumption.add(event.getEnergy().add(predIndex.negate()));
        this.predIndex = event.getEnergy();
        this.count = this.count + 1;
        return this;
    }

    public EecData calculateMeanPower(){
        this.meanPower = this.meanPower.divide(new BigDecimal(count),2, BigDecimal.ROUND_HALF_UP);
        return this;
    }
}
