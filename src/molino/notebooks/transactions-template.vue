<template>
  <v-data-table
      dense
      :headers="headers"
      :items="transactions"
      :penalty_threshold.sync="penalty_threshold"
      :options.sync="options"
      :filters.sync="filters"
      :server-items-length="transactions_count"
      :penalty_range="penalty_range"
      :watermark_range="watermark_range"
  >
    <template v-slot:top="{ info }">
      <v-card>
        <v-card-title>
          Filters
        </v-card-title>
        <v-card-text>
          <v-form>
            <!-- Filter on Multi-transaction name -->
            <v-text-field
                placeholder="Name"
                @change="(v) => update_filter(['mtr', v])"
                clearable
                dense
            />
            <!-- Filter on Victim transaction name -->
            <v-text-field
                placeholder="Victim"
                @change="(v) => update_filter(['tr', v])"
                clearable
                dense
            />
            <!-- Filter on Multi-transaction size -->
            <v-autocomplete
                label="Size"
                chips
                clearable
                deletable-chips
                multiple
                small-chips
                :disabled="size_range.length < 2"
                :items="[...Array(size_range[1]).keys()].map(i => i + 1)"
                @change="(v) => update_filter(['size', v])"
            />
            <!-- Filter on Classification-->
            <v-input
                label="Classification"
            >
              <v-btn-toggle
                  label="Classification"
                  multiple
                  dense
                  @change="(v) => update_filter(['interfering', v])"
                  active-class="primary"
              >
                <v-btn value="True">
                  ITF
                </v-btn>
                <v-btn value="False">
                  Free
                </v-btn>
                <!-- FIXME value set to null or no value does not send a `None` to Python which is the classif to find. -->
                <v-btn value="None">
                  N.A.
                </v-btn>
              </v-btn-toggle>

            </v-input>
            <!-- Filter on HWM -->
            <v-range-slider
                label="HWM"
                :disabled="watermark_range.length < 2"
                :min="watermark_range[0]"
                :max="watermark_range[1]"
                thumb-label=true
                @change="(v) => update_filter(['hwm', v])"
            />
            <!-- Filter on LWM -->
            <v-range-slider
                label="LWM"
                :disabled="watermark_range.length < 2"
                :min="watermark_range[0]"
                :max="watermark_range[1]"
                thumb-label=true
                @change="(v) => update_filter(['lwm', v])"
            />
            <!-- Filter on Penalty -->
            <v-range-slider
                label="Penalty"
                :disabled="penalty_range.length < 2"
                :min="penalty_range[0]"
                :max="penalty_range[1]"
                thumb-label=true
                step=0.1
                @change="(v) => {update_filter(['penalty_hwm', v]); update_filter(['penalty_lwm', v])}"
            >
              <template v-slot:thumb-label="{ value }">
                {{ (penalty_mode * value * 100).toFixed(0) }}%
              </template>
            </v-range-slider>
            <!-- Configure ITF classification threshold -->
            <v-slider
                label="ITF Threshold"
                :disabled="penalty_range.length < 2"
                :min="0"
                :max="Math.max(Math.abs(penalty_range[0]), Math.abs(penalty_range[1]))"
                thumb-label="always"
                v-model="penalty_threshold"
                step=0.01
            >
              <template v-slot:thumb-label="{ value }">
                {{ (value * 100).toFixed(0) }}%
              </template>
            </v-slider>

            <!-- Configure penalty mode-->
            <v-radio-group
                v-model="penalty_mode"
                row
                mandatory
            >
              <v-radio
                  label="Timing (lower is better)"
                  value="-1"
              ></v-radio>
              <v-radio
                  label="Bandwidth (higher is better)"
                  value="1"
              ></v-radio>
            </v-radio-group>

          </v-form>
        </v-card-text>
      </v-card>

      <v-toolbar>
        <v-dialog v-model="hist_dialog" max-width="800px">
          <v-card>
            <v-card-title class="text-h2">Observations for {{ hist_item.tr }}</v-card-title>
            <v-card-subtitle class="text-h3">In the context of {{hist_item.mtr_name}}</v-card-subtitle>
            <!-- Timing histograms in isolation and contention -->
            <v-card-text>
              <v-container>
                <v-row>
                  <v-spacer />
                  <v-col cols="6" class="text-center">
                    <div class="text-h3">{{hist_item.tr}} Interference</div>
                    <v-sheet outlined shaped>
                      <v-sparkline
                          smooth="2"
                          type="bar"
                          :value="hist_itf"
                          :labels="hist_labels"
                      >
                      </v-sparkline>
                    </v-sheet>
                  </v-col>
                  <v-spacer />
                </v-row>
                <v-row>
                  <v-spacer />
                  <v-col cols="6">
                    <div class="text-h3 text-center">{{hist_item.tr}} in Isolation</div>
                    <v-sheet outlined shaped>
                      <v-sparkline
                          smooth="2"
                          type="bar"
                          :value="hist_iso"
                          :labels="hist_labels"
                      >
                      </v-sparkline>
                    </v-sheet>
                  </v-col>
                  <v-spacer />
                </v-row>
              </v-container>

            </v-card-text>
            <!--            <v-card-actions>-->
            <!--              <v-spacer></v-spacer>-->
            <!--              <v-btn color="blue darken-1" text >Close</v-btn>-->
            <!--              <v-spacer></v-spacer>-->
            <!--            </v-card-actions>-->
          </v-card>
        </v-dialog>
      </v-toolbar>
    </template>

    <!-- Multi-transaction name display -->
    <template v-slot:item.mtr="{ item }">
      <v-chip-group column>
        <v-chip
            v-for="tr in item.mtr"
            :color="(tr == item.tr) ? 'accent': 'default'"
        >
          <v-icon left v-if="tr == item.tr">
            mdi-target
          </v-icon>
          {{ tr }}
        </v-chip>
      </v-chip-group>
    </template>

    <!-- HWM name display -->
    <template v-slot:item.hwm="{ item }">
      <v-tooltip bottom>
        <template v-slot:activator="{ on, attrs }">
          <span
              v-bind="attrs"
              v-on="on"
          >
            {{ item.timing.hwm.value.toFixed(2) }}
          </span>
        </template>
        <v-chip-group
            column
        >
          <v-chip v-for="j in item.timing.hwm.justifications">
            {{ j.file }}:{{ j.line }}
          </v-chip>
        </v-chip-group>
      </v-tooltip>
    </template>

    <!-- LWM name display -->
    <template v-slot:item.lwm="{ item }">
      <v-tooltip bottom>
        <template v-slot:activator="{ on, attrs }">
          <span
              v-bind="attrs"
              v-on="on"
          >
            {{ item.timing.lwm.value.toFixed(2) }}
          </span>
        </template>
        <v-chip-group
            column
        >
          <v-chip v-for="j in item.timing.lwm.justifications">
            {{ j.file }}:{{ j.line }}
          </v-chip>
        </v-chip-group>
      </v-tooltip>
    </template>

    <!-- Isolation reference name display -->
    <template v-slot:item.ref_hwm="{ item }">
      <v-tooltip bottom>
        <template v-slot:activator="{ on, attrs }">
          <span
              v-bind="attrs"
              v-on="on"
          >
            {{ item.timing_isolation.hwm.value.toFixed(2) }}
          </span>
        </template>
        <v-chip-group
            column
        >
          <v-chip v-for="j in item.timing_isolation.hwm.justifications">
            {{ j.file }}:{{ j.line }}
          </v-chip>
        </v-chip-group>
      </v-tooltip>
    </template>

    <!-- Isolation reference name display -->
    <template v-slot:item.ref_lwm="{ item }">
      <v-tooltip bottom>
        <template v-slot:activator="{ on, attrs }">
          <span
              v-bind="attrs"
              v-on="on"
          >
            {{ item.timing_isolation.lwm.value.toFixed(2) }}
          </span>
        </template>
        <v-chip-group
            column
        >
          <v-chip v-for="j in item.timing_isolation.lwm.justifications">
            {{ j.file }}:{{ j.line }}
          </v-chip>
        </v-chip-group>
      </v-tooltip>
    </template>

    <!-- Penalty name display -->
    <template v-slot:item.penalty_hwm="{ item }">
      <v-chip
          :class="(Math.abs(item.penalty_hwm) > penalty_threshold) ? 'warning': 'default'"
      >
        {{ (penalty_mode * item.penalty_hwm * 100).toFixed(2) }}%
      </v-chip>
    </template>

    <!-- Penalty name display -->
    <template v-slot:item.penalty_lwm="{ item }">
      <v-chip
          :class="(Math.abs(item.penalty_lwm) > penalty_threshold) ? 'warning': 'default'"
      >
        {{ (penalty_mode * item.penalty_lwm * 100).toFixed(2) }}%
      </v-chip>
    </template>

    <!-- Classification display -->
    <template v-slot:item.interfering="{ item }">
      <v-chip v-if="item.interfering.interfering == false" color="success">
        <v-icon small>
          mdi-check-bold
        </v-icon>
        Free
      </v-chip>
      <v-chip v-else-if="item.interfering.interfering == true" color="warning">
        <v-icon small>
          mdi-skull-outline
        </v-icon>
        ITF
      </v-chip>
      <v-chip v-else color="default">
        <v-icon small>
          mdi-help-circle-outline
        </v-icon>
        N.A.
      </v-chip>
    </template>

    <!-- Item actions -->
    <template v-slot:item.actions="{ item }">
      <v-icon
          small
          class="mr-2"
          @click="update_timing_histogram(item)"
      >
        mdi-chart-histogram
      </v-icon>
    </template>
  </v-data-table>

</template>
<script setup lang="ts">
</script>