/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>

#include "redis.h"
#include "percentile.h"

/* Initialize or reset a sampleReservoire */
void initReservoire(sampleReservoire* samples) {
    samples->totalItems = 0;
    samples->numSamples = 0;
    if (samples->samples != NULL) {
        zfree(samples->samples);
        samples->samples = NULL;
    }
}

/* Sample an item into a reservoire. If this is the first sample in the
 * reservoire, the array of samples will be allocated. All items will be
 * included in the reservoire until the maximum number of samples is reached,
 * at which point new samples will be randomly selected for inclusion (and
 * eviction). This alrogithm gurantees that any given sample has a 1/N chance
 * of being in the reservoire, for N total items seen */
void sampleItem(sampleReservoire* samples, sample_t item) {
    if (samples->samples == NULL) {
        samples->samples = zmalloc(sizeof(sample_t) * PERCENTILE_NUM_SAMPLES);
    }

    if (samples->numSamples < PERCENTILE_NUM_SAMPLES) {
        samples->samples[samples->numSamples++] = item;
    }
    else {
        long r = random() % (samples->totalItems);
        if (r < PERCENTILE_NUM_SAMPLES) {
            samples->samples[r] = item;
        }
    }

    samples->totalItems++;
}

int compare_sample_t(const void *a, const void *b) {
    const sample_t *lla = (const sample_t*) a;
    const sample_t *llb = (const sample_t*) b;
    return (*lla > *llb) - (*lla < *llb);
}

/* Given a reservoire of samples and a list of percent values, calculate the
 * corresponding percentiles. */
void calculatePercentiles(
        sampleReservoire* samples,
        uint numPercentiles,
        const double* percentiles,
        sample_t* results) {
    // No samples in the reservoire.
    if (samples->samples == NULL) {
        for (uint i = 0; i < numPercentiles; i++) {
          results[i] = 0;
        }
        return;
    }

    // sort samples
    qsort(samples->samples, samples->numSamples, sizeof(sample_t), compare_sample_t);

    // extract the percentiles
    for (uint i = 0; i < numPercentiles; i++) {
        results[i] = samples->samples[(int)(samples->numSamples * percentiles[i])];
    }

}
