import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Delete,
  Put,
  ParseIntPipe,
} from '@nestjs/common';
import { ArrLogManualsService } from './arr-log-manuals.service';
import { CreateArrLogManualDto } from './dto/create-arr-log-manual.dto';
import { UpdateArrLogManualDto } from './dto/update-arr-log-manual.dto';

@Controller('arr-log-manuals')
export class ArrLogManualsController {
  constructor(private readonly service: ArrLogManualsService) {}

  @Post()
  async create(@Body() dto: CreateArrLogManualDto) {
    const created = await this.service.create(dto);
    return {
      message: 'Log successfully created',
      data: created,
    };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return {
      message: 'All data fetched successfully',
      data,
    };
  }

  @Get(':id')
  async findOne(@Param('id', ParseIntPipe) id: number) {
    const data = await this.service.findOne(id);
    return {
      message: `Data fetched for ID ${id}`,
      data,
    };
  }

  @Put(':id')
  async update(
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: UpdateArrLogManualDto,
  ) {
    const data = await this.service.update(id, dto);
    return {
      message: `Data successfully updated for ID ${id}`,
      data,
    };
  }

  @Delete(':id')
  async remove(@Param('id', ParseIntPipe) id: number) {
    const data = await this.service.remove(id);
    return {
      message: `Data successfully deleted for ID ${id}`,
      data,
    };
  }
}
