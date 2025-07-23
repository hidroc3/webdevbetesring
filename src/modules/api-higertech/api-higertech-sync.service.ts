import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import * as dayjs from 'dayjs';
import { AwlrStationsService } from '@/modules/awlr-stations/awlr-stations.service';
import { ArrStationsService } from '@/modules/arr-stations/arr-stations.service';
import { Cron, CronExpression } from '@nestjs/schedule';

interface HigertechReading {
  device_id: string;
  reading_at: string;
  water_level?: string;
  rainfall?: string;
  battery?: string;
}

@Injectable()
export class HigertechSyncService {
  private readonly logger = new Logger(HigertechSyncService.name);

  private awlrMapping: Record<string, string> = {
    HGT412: 'Pabuaran',
    HGT281: 'Al Azhar Kaujon',
    HGT414: 'Pamarayan Hulu',
    HGT280: 'Kenari Kasunyatan',
    HGT282: 'Jembatan Cimake',
    HGT278: 'Bendung Karet Cibanten',
    HGT413: 'Kp. Peusar',
    HGT664: 'Bojong Manik',
    HGT671: 'Cikande',
    HGT678: 'Jasinga',
    HGT679: 'Bendungan Karet Cidurian',
    HGT709: 'Tanjungsari',
  };

  private arrMapping: Record<string, string> = {
    HGT665: 'Bojong Manik',
  };

  constructor(
    private readonly httpService: HttpService,
    private readonly awlrService: AwlrStationsService,
    private readonly arrService: ArrStationsService,
  ) {}

  @Cron(CronExpression.EVERY_5_MINUTES)
  async handleCron() {
    this.logger.log('Running Higertech sync...');
    try {
      await this.sync();
      this.logger.log('Higertech sync complete');
    } catch (err) {
      this.logger.error('Higertech sync failed', err);
    }
  }

  // @Cron('*/30 * * * * *') // Setiap 30 detik
  // async handleCron() {
  //   this.logger.log('Running Higertech sync...');
  //   try {
  //     await this.sync();
  //     this.logger.log('Higertech sync complete');
  //   } catch (err) {
  //     this.logger.error('Higertech sync failed', err);
  //   }
  // }

  async sync() {
    await this.syncAwlr();
    await this.syncArr();
  }

  private async fetchDeviceData(
    deviceId: string,
  ): Promise<HigertechReading | null> {
    const url = `https://api.higertech.com/v2/reading/device/${deviceId}`;
    try {
      const { data } = await firstValueFrom(this.httpService.get(url));
      const result = Array.isArray(data?.response)
        ? data.response.find((d) => d.device_id === deviceId)
        : null;
      if (!result) this.logger.warn(`No data for device ${deviceId}`);
      return result;
    } catch (error) {
      this.logger.error(`Failed to fetch data for device ${deviceId}`, error);
      return null;
    }
  }

  private async syncAwlr() {
    for (const deviceId of Object.keys(this.awlrMapping)) {
      const postName = this.awlrMapping[deviceId];
      const data = await this.fetchDeviceData(deviceId);
      if (!data || !data.reading_at || data.water_level == null) {
        this.logger.warn(`Incomplete AWLR data for device ${deviceId}`);
        continue;
      }

      const payload = {
        time: dayjs(data.reading_at).add(7, 'hour').toDate(),
        water_level: Number(data.water_level),
        battery: data.battery ? Number(data.battery) : undefined,
        post_name: postName,
      };

      try {
        await this.awlrService.updateByDeviceId(deviceId, payload);
        this.logger.log(`AWLR updated: ${postName} (${deviceId})`);
      } catch (err) {
        this.logger.warn(
          `Failed to update AWLR ${postName} (${deviceId})`,
          err,
        );
      }
    }
  }

  private async syncArr() {
    for (const deviceId of Object.keys(this.arrMapping)) {
      const postName = this.arrMapping[deviceId];
      const data = await this.fetchDeviceData(deviceId);
      if (!data || !data.reading_at || data.rainfall == null) {
        this.logger.warn(`Incomplete ARR data for device ${deviceId}`);
        continue;
      }

      const payload = {
        time: dayjs(data.reading_at).add(7, 'hour').toDate(),
        rainfall: Number(data.rainfall),
        battery: data.battery ? Number(data.battery) : undefined,
        post_name: postName,
      };

      try {
        await this.arrService.updateByDeviceId(deviceId, payload);
        this.logger.log(`ARR updated: ${postName} (${deviceId})`);
      } catch (err) {
        this.logger.warn(`Failed to update ARR ${postName} (${deviceId})`, err);
      }
    }
  }
}
