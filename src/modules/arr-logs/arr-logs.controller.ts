import {
  Controller,
  Get,
  Post,
  Patch,
  Delete,
  Param,
  Body,
} from '@nestjs/common';
import { ArrLogsService } from './arr-logs.service';
import { CreateArrLogDto } from './dto/create-arr-log.dto';
import { UpdateArrLogDto } from './dto/update-arr-log.dto';

@Controller('arr-logs')
export class ArrLogsController {
  constructor(private readonly arrLogsService: ArrLogsService) {}

  // POST /arr-logs
  @Post()
  async create(@Body() dto: CreateArrLogDto) {
    const created = await this.arrLogsService.create(dto);
    const { id, ...rest } = created;
    return {
      message: 'Log successfully created',
      data: rest,
    };
  }

  // 🔸 GET /arr-logs — semua log hari ini (semua stasiun)
  @Get()
  async findAllToday() {
    const logs = await this.arrLogsService.findAllToday();
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: "Today's data fetched successfully (WIB)",
      data: filtered,
    };
  }

  // 🔹 GET /arr-logs/all — semua log semua stasiun
  @Get('all')
  async findAll() {
    const logs = await this.arrLogsService.findAll();
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: 'All data fetched successfully',
      data: filtered,
    };
  }

  // 🔸 GET /arr-logs/station/:arr_station_id — log hari ini (per stasiun)
  @Get('station/:arr_station_id')
  async findTodayByStation(@Param('arr_station_id') arr_station_id: string) {
    const logs =
      await this.arrLogsService.findTodayByStationId(+arr_station_id);
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: `Today's data fetched for station ${arr_station_id} (WIB)`,
      data: filtered,
    };
  }

  // 🔹 GET /arr-logs/station/:arr_station_id/all — semua log per stasiun
  @Get('station/:arr_station_id/all')
  async findAllByStation(@Param('arr_station_id') arr_station_id: string) {
    const logs = await this.arrLogsService.findAllByStationId(+arr_station_id);
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: `All data fetched for station ${arr_station_id}`,
      data: filtered,
    };
  }

  // PATCH /arr-logs/station/:arr_station_id
  @Patch('station/:arr_station_id')
  async update(
    @Param('arr_station_id') arr_station_id: string,
    @Body() dto: UpdateArrLogDto,
  ) {
    const result = await this.arrLogsService.updateByStationId(
      +arr_station_id,
      dto,
    );
    return {
      message: `Logs successfully updated for station ${arr_station_id}`,
      data: result,
    };
  }

  // DELETE /arr-logs/station/:arr_station_id
  @Delete('station/:arr_station_id')
  async remove(@Param('arr_station_id') arr_station_id: string) {
    const result = await this.arrLogsService.removeByStationId(+arr_station_id);
    return {
      message: `Logs successfully deleted for station ${arr_station_id}`,
      data: result,
    };
  }
}
