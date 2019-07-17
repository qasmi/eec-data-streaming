package com.squaresense.eecdatastreaming;

import lombok.Data;

import java.math.BigDecimal;

@Data
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

    public EecData aggregate(EecDataEvent event){
        this.minPower = event.getPower().min(minPower);
        this.maxPower = event.getPower().max(maxPower);
        this.meanPower = this.meanPower.add(event.getPower());
        this.energyConsumption = energyConsumption.add(event.getEnergy().add(predIndex.negate()));
        this.predIndex = event.getEnergy();
        this.count = this.count + 1;
        return this;
    }

    public EecData updateEecMetrics(){
        this.meanPower = this.meanPower.divide(new BigDecimal(count),2, BigDecimal.ROUND_HALF_UP);
        return this;
    }
}
