package com.squaresense.eecdatastreaming;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EecDataEvent {

    private String meter;
    private Instant date;
    private BigDecimal energy;
    private BigDecimal power;
}
