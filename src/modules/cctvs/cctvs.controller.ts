import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { CctvsService } from './cctvs.service';
import { CreateCctvDto } from './dto/create-cctv.dto';
import { UpdateCctvDto } from './dto/update-cctv.dto';

@Controller('cctvs')
export class CctvsController {
  constructor(private readonly service: CctvsService) {}

  @Post()
  async create(@Body() dto: CreateCctvDto) {
    const data = await this.service.create(dto);
    return {
      message: 'CCTV successfully created',
      data,
    };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return {
      message: 'All CCTVs fetched successfully',
      data,
    };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return {
      message: `CCTV fetched for ID ${id}`,
      data,
    };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateCctvDto) {
    const data = await this.service.update(+id, dto);
    return {
      message: `CCTV updated for ID ${id}`,
      data,
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return {
      message: `CCTV deleted for ID ${id}`,
      data,
    };
  }
}
