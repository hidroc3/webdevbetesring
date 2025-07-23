import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import * as dayjs from 'dayjs';
import { AwlrStationsService } from '@/modules/awlr-stations/awlr-stations.service';
import { ArrStationsService } from '@/modules/arr-stations/arr-stations.service';
import { Cron, CronExpression } from '@nestjs/schedule';

@Injectable()
export class AptechSyncService {
  private readonly logger = new Logger(AptechSyncService.name);

  private awlrMapping: Record<string, string> = {
    sabagi: 'Sabagi',
    undarandir: 'Undar Andir',
  };

  private arrMapping: Record<string, string> = {
    ciminyak: 'Ciminyak',
    pchciomas: 'Ciomas',
    kiarasari: 'Kiarasari',
    aptechv2_h3: 'Padarincang',
    aptechv2_h2: 'Pamarayan',
    aptechv2_f1: 'Pulo Ampel',
    sepang: 'Sepang',
    smp2lewudamar: 'SMP2 Leuwidamar',
    sukmajaya: 'Sukmajaya',
    telagaluhur: 'Telaga Luhur',
    tersaba: 'Tersaba',
    toge: 'Toge',
  };

  private apiUrl =
    'https://sdatelemetry.com/API_ap_telemetry/datatelemetry.php?idbbws=2&user=sdatelem_icuadm&pass=Icupu2015';

  constructor(
    private readonly httpService: HttpService,
    private readonly awlrService: AwlrStationsService,
    private readonly arrService: ArrStationsService,
  ) {}

  @Cron(CronExpression.EVERY_5_MINUTES)
  async handleCron() {
    this.logger.log('Running Aptech sync...');
    try {
      await this.sync();
      this.logger.log('Aptech sync complete');
    } catch (err) {
      this.logger.error('Aptech sync failed', err);
    }
  }

  async sync() {
    const { data } = await firstValueFrom(this.httpService.get(this.apiUrl));
    const list = data.telemetryjakarta;

    await this.syncAwlr(list);
    await this.syncArr(list);
  }

  private async syncAwlr(data: any[]) {
    for (const item of data) {
      const deviceId = item.nama_lokasi;
      if (!this.awlrMapping[deviceId]) continue;

      const postName = this.awlrMapping[deviceId];
      const time = dayjs(`${item.ReceivedDate} ${item.ReceivedTime}`)
        .add(7, 'hour') // convert to WIB
        .toDate();
      const waterLevel = Number(item.WLevel) / 100;

      try {
        await this.awlrService.updateByDeviceId(deviceId, {
          time,
          water_level: waterLevel,
          post_name: postName,
        });
        this.logger.log(`AWLR updated: ${postName} (${deviceId})`);
      } catch (err) {
        this.logger.warn(
          `Failed to update AWLR ${postName} (${deviceId})`,
          err,
        );
      }
    }
  }

  private async syncArr(data: any[]) {
    for (const item of data) {
      const deviceId = item.nama_lokasi;
      if (!this.arrMapping[deviceId]) continue;

      const postName = this.arrMapping[deviceId];
      const time = dayjs(`${item.ReceivedDate} ${item.ReceivedTime}`)
        .add(7, 'hour') // convert to WIB
        .toDate();
      const rainfall = Number(item.Rain);

      try {
        await this.arrService.updateByDeviceId(deviceId, {
          time,
          rainfall,
          post_name: postName,
        });
        this.logger.log(`ARR updated: ${postName} (${deviceId})`);
      } catch (err) {
        this.logger.warn(`Failed to update ARR ${postName} (${deviceId})`, err);
      }
    }
  }
}
