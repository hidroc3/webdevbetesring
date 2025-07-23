import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { AwlrLogsService } from './awlr-logs.service';
import { CreateAwlrLogDto } from './dto/create-awlr-log.dto';
import { UpdateAwlrLogDto } from './dto/update-awlr-log.dto';

@Controller('awlr-logs')
export class AwlrLogsController {
  constructor(private readonly service: AwlrLogsService) {}

  @Post()
  async create(@Body() dto: CreateAwlrLogDto) {
    const created = await this.service.create(dto);
    const { id, ...rest } = created;
    return {
      message: 'Log successfully created',
      data: rest,
    };
  }

  @Get()
  async findAllToday() {
    const logs = await this.service.findAllToday();
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: "Today's data fetched successfully (WIB)",
      data: filtered,
    };
  }

  @Get('all')
  async findAll() {
    const logs = await this.service.findAll();
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: 'All data fetched successfully',
      data: filtered,
    };
  }

  @Get('station/:awlr_station_id')
  async findTodayByStation(@Param('awlr_station_id') awlr_station_id: string) {
    const logs = await this.service.findTodayByStationId(+awlr_station_id);
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: `Today's data fetched for station ${awlr_station_id} (WIB)`,
      data: filtered,
    };
  }

  @Get('station/:awlr_station_id/all')
  async findAllByStation(@Param('awlr_station_id') awlr_station_id: string) {
    const logs = await this.service.findAllByStationId(+awlr_station_id);
    const filtered = logs.map(({ id, ...rest }) => rest);
    return {
      message: `All data fetched for station ${awlr_station_id}`,
      data: filtered,
    };
  }

  @Patch('station/:awlr_station_id')
  async update(
    @Param('awlr_station_id') awlr_station_id: string,
    @Body() dto: UpdateAwlrLogDto,
  ) {
    const result = await this.service.updateByStationId(+awlr_station_id, dto);
    return {
      message: `Logs successfully updated for station ${awlr_station_id}`,
      data: result,
    };
  }

  @Delete('station/:awlr_station_id')
  async remove(@Param('awlr_station_id') awlr_station_id: string) {
    const result = await this.service.removeByStationId(+awlr_station_id);
    return {
      message: `Logs successfully deleted for station ${awlr_station_id}`,
      data: result,
    };
  }
}
