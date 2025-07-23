import { Injectable } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { lastValueFrom } from 'rxjs';
import { SirineService } from '../sirine/sirine.service';
import { CreateSirineDto } from '../sirine/dto/create-sirine.dto';
import { ConfigService } from '@nestjs/config'; // ✅ Import ConfigService

@Injectable()
export class ApiSirineEagleioService {
  constructor(
    private readonly http: HttpService,
    private readonly sirineService: SirineService,
    private readonly config: ConfigService, // ✅ Inject ConfigService
  ) {}

  async fetchAndStoreSirineData() {
    const eagleApiKey = this.config.get<string>('EAGLE_API_KEY'); // ✅ Ambil dari .env

    const sirineList = [
      {
        name: 'Sirine 1 (Undar Andir)',
        address: 'Kragilan',
        latitude: -6.151087,
        longitude: 106.307451,
        url1: 'https://api.eagle.io/api/v1/nodes/6312c3fc93d9e0805ff8c780',
        url2: 'https://api.eagle.io/api/v1/nodes/6312c4e1e8039a48e141638c',
        url3: 'https://api.eagle.io/api/v1/nodes/6312c52393d9e0805ff9321a',
        url4: 'https://api.eagle.io/api/v1/nodes/6312c52393d9e0805ff9321e',
      },
      {
        name: 'Sirine 2 (Walikukun)',
        address: 'Walikukun',
        latitude: -6.089889,
        longitude: 106.299912,
        url1: 'https://api.eagle.io/api/v1/nodes/63a04a324402dc25cac3b33e',
        url2: 'https://api.eagle.io/api/v1/nodes/63a04a324402dc25cac3b363',
        url3: 'https://api.eagle.io/api/v1/nodes/63a04a324402dc25cac3b387',
        url4: 'https://api.eagle.io/api/v1/nodes/63a04a324402dc25cac3b385',
      },
      {
        name: 'Sirine 3 (Puser)',
        address: 'Puser',
        latitude: -6.037305,
        longitude: 106.319125,
        url1: 'https://api.eagle.io/api/v1/nodes/63c0e27a9e515eadabe6c0af',
        url2: 'https://api.eagle.io/api/v1/nodes/63c0e27a9e515eadabe6c0d4',
        url3: 'https://api.eagle.io/api/v1/nodes/63c0e27a9e515eadabe6c0f4',
        url4: 'https://api.eagle.io/api/v1/nodes/63c0e27a9e515eadabe6c0f2',
      },
    ];

    for (const [index, s] of sirineList.entries()) {
      const headers = { 'x-api-key': eagleApiKey }; // ✅ pakai dari .env

      const [r1, r2, r3, r4] = await Promise.all([
        lastValueFrom(this.http.get(s.url1, { headers })),
        lastValueFrom(this.http.get(s.url2, { headers })),
        lastValueFrom(this.http.get(s.url3, { headers })),
        lastValueFrom(this.http.get(s.url4, { headers })),
      ]);

      const data1 = r1.data;
      const data2 = r2.data;
      const data3 = r3.data;
      const data4 = r4.data;

      const onState =
        data3.states.find((x: any) => x._id === data3.currentStateId)?.name ??
        null;
      const offState =
        data4.states.find((x: any) => x._id === data4.currentStateId)?.name ??
        null;

      const dto: CreateSirineDto = {
        name: s.name,
        address: s.address,
        latitude: s.latitude,
        longitude: s.longitude,
        device_name: data1.name,
        firmware_version: data1.firmwareVersion,
        status_logger: data1.currentStatus,
        last_comm_success: data1.lastCommsSuccess
          ? new Date(data1.lastCommsSuccess)
          : undefined,
        is_active: data1.isActive,
        outdated_alarm_status: data1.alarms?.outdatedAlarm?.status?.alarmState,
        outdated_data_time: data1.alarms?.outdatedAlarm?.status?.dataTime
          ? new Date(data1.alarms.outdatedAlarm.status.dataTime)
          : undefined,
        battery_voltage: data2.currentValue,
        battery_unit: data2.units,
        battery_status_alarm: data2.states?.[0]?.name,
        on_control_value: data3.currentValue,
        on_control_state: onState,
        off_control_value: data4.currentValue,
        off_control_state: offState,

        on_control_url: s.url3,
        on_control_state_id: data3.currentStateId,
        off_control_url: s.url4,
        off_control_state_id: data4.currentStateId,

        is_sirine_on: false,
      };

      const id = BigInt(index + 1);
      await this.sirineService.update(id, dto);
    }
  }
}
