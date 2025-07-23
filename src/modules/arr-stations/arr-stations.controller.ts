import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { ArrStationsService } from './arr-stations.service';
import { CreateArrStationDto } from './dto/create-arr-station.dto';
import { UpdateArrStationDto } from './dto/update-arr-station.dto';

@Controller('arr-stations')
export class ArrStationsController {
  constructor(private readonly arrStationsService: ArrStationsService) {}

  @Post()
  async create(@Body() dto: CreateArrStationDto) {
    const created = await this.arrStationsService.create(dto);
    const { id, ...rest } = created;
    return {
      message: 'Station successfully created',
      data: rest,
    };
  }

  @Get()
  async findAll() {
    const stations = await this.arrStationsService.findAll();
    const filtered = stations.map(({ id, ...rest }) => rest);
    return {
      message: 'All station data fetched successfully',
      data: filtered,
    };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const station = await this.arrStationsService.findOne(+id);
    if (!station) {
      return {
        message: `Station with ID ${id} not found`,
        data: null,
      };
    }
    const { id: _, ...rest } = station;
    return {
      message: `Station ${id} data fetched successfully`,
      data: rest,
    };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateArrStationDto) {
    const updated = await this.arrStationsService.update(+id, dto);
    const { id: _, ...rest } = updated;
    return {
      message: `Station ${id} updated successfully`,
      data: rest,
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const deleted = await this.arrStationsService.remove(+id);
    const { id: _, ...rest } = deleted;
    return {
      message: `Station ${id} deleted successfully`,
      data: rest,
    };
  }
}
